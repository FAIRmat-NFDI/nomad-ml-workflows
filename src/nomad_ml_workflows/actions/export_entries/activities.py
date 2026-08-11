import json
import multiprocessing
import shutil
import zipfile
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from nomad.actions.manager import action_instance_artifacts_dir
from nomad.app.v1.models.models import MetadataPagination, MetadataRequired
from nomad.config import config as nomad_config
from nomad.files import StagingUploadFiles
from nomad.search import search as nomad_search
from nomad.uploads import get_upload_files
from nomad.utils import get_logger
from temporalio import activity

from nomad_ml_workflows.actions.export_entries.models import (
    CleanupArtifactsInput,
    ExportDatasetInput,
    ManifestEntry,
    ManifestFile,
    MetadataFile,
    OutputFile,
    PrepareManifestInput,
    PrepareManifestOutput,
    ReadArchivesWorkflowInput,
    WriteMetadataFileInput,
)
from nomad_ml_workflows.actions.export_entries.utils import (
    generate_archives,
    require_pyarrow,
    worker_process_initializer,
    write_dicts_to_json,
    write_table_rows_to_ndjson,
    write_table_rows_to_tabular_file,
)

config = nomad_config.get_plugin_entry_point(
    'nomad_ml_workflows.actions:export_entries'
)
logger = get_logger(__name__)

DATA_FILE_NAME = 'data'
MANIFEST_FILE_NAME = 'selected_entries'
METADATA_FILE_NAME = 'metadata'


@activity.defn
def prepare_manifest(data: PrepareManifestInput) -> PrepareManifestOutput:
    max_num_entries_limit = min(
        config.max_entries_export_limit,  # type: ignore
        data.num_entries_user_limit,
    )
    manifest: list = []
    page_size = min(10000, max_num_entries_limit)
    starttime = datetime.now(timezone.utc).isoformat()
    response = nomad_search(
        user_id=data.user_id,
        owner=data.owner,
        query=data.query,
        required=MetadataRequired(include=['entry_id', 'upload_id']),  # type: ignore
        pagination=MetadataPagination(page_size=page_size),  # type: ignore
    )
    num_entries_available = response.pagination.total
    while True:
        manifest.extend(
            [
                {'entry_id': entry['entry_id'], 'upload_id': entry['upload_id']}
                for entry in response.data
            ]
        )
        if len(manifest) >= max_num_entries_limit:
            break
        if response.pagination.next_page_after_value is None:
            # last page was already consumed
            break
        response = nomad_search(
            user_id=data.user_id,
            owner=data.owner,
            query=data.query,
            required=MetadataRequired(include=['entry_id', 'upload_id']),  # type: ignore
            pagination=MetadataPagination(
                page_size=page_size,
                page_after_value=response.pagination.next_page_after_value,
            ),  # type: ignore
        )
    endtime = datetime.now(timezone.utc).isoformat()

    reached_max_entries_limit = num_entries_available > max_num_entries_limit
    manifest = manifest[:max_num_entries_limit]
    num_entries_selected = len(manifest)

    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    manifest_file_path = artifacts_subdirectory / f'{MANIFEST_FILE_NAME}.json'
    write_dicts_to_json(manifest, manifest_file_path)

    manifest_file = ManifestFile(
        file_path=manifest_file_path.as_posix(),
        file_size=manifest_file_path.stat().st_size,
    )

    return PrepareManifestOutput(
        search_start_time=starttime,
        search_end_time=endtime,
        num_entries_available=num_entries_available,
        num_entries_selected=num_entries_selected,
        reached_max_entries_limit=reached_max_entries_limit,
        manifest_file=manifest_file,
    )


@activity.defn
def read_archives_and_write_output_json(
    data: ReadArchivesWorkflowInput,
) -> OutputFile:
    """
    Reads the archives and writes the output JSON file.
    """
    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    manifest_file_path = artifacts_subdirectory / f'{MANIFEST_FILE_NAME}.json'
    output_file_path = artifacts_subdirectory / f'{DATA_FILE_NAME}.json'
    temporary_output_file_path = output_file_path.with_stem(
        f'{output_file_path.stem}.tmp'
    )

    # load manifest
    with open(manifest_file_path, encoding='utf-8') as f:
        manifest = [ManifestEntry(**entry) for entry in json.load(f)]

    info = activity.info()
    activity_logger = logger.bind(activity_type=info.activity_type)

    archives = generate_archives(manifest, data.required, data.user_id, activity_logger)
    num_entries_exported = write_dicts_to_json(archives, temporary_output_file_path)
    temporary_output_file_path.replace(output_file_path)

    return OutputFile(
        file_path=output_file_path.as_posix(),
        file_size=output_file_path.stat().st_size,
        num_entries_exported=num_entries_exported,
    )


def _read_archives_and_write_output_tabular(
    data: ReadArchivesWorkflowInput, activity_type: str
) -> OutputFile:
    activity_logger = logger.bind(activity_type=activity_type)
    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    manifest_file_path = artifacts_subdirectory / f'{MANIFEST_FILE_NAME}.json'
    table_rows_file_path = artifacts_subdirectory / 'table_rows.tmp.ndjson'
    output_file_path = (
        artifacts_subdirectory / f'{DATA_FILE_NAME}.{data.output_file_format}'
    )
    temporary_output_file_path = output_file_path.with_stem(
        f'{output_file_path.stem}.tmp'
    )

    # load manifest
    with open(manifest_file_path, encoding='utf-8') as f:
        manifest = [ManifestEntry(**entry) for entry in json.load(f)]

    activity_logger.info('Reading archives and building the schema...')
    columns_quantity_def = write_table_rows_to_ndjson(
        manifest, data.required, data.user_id, table_rows_file_path, activity_logger
    )
    activity_logger.info('Writing table rows to tabular file...')
    num_entries_exported = write_table_rows_to_tabular_file(
        table_rows_file_path,
        temporary_output_file_path,
        columns_quantity_def,
        max_buffer_bytes=config.max_write_buffer_size_bytes,  # type: ignore
        logger=activity_logger,
    )
    temporary_output_file_path.replace(output_file_path)
    activity_logger.info(
        f'{num_entries_exported} table rows written to '
        f'"data.{data.output_file_format}" file.'
    )

    return OutputFile(
        file_path=output_file_path.as_posix(),
        file_size=output_file_path.stat().st_size,
        num_entries_exported=num_entries_exported,
    )


@activity.defn
def read_archives_and_write_output_tabular(
    data: ReadArchivesWorkflowInput,
) -> OutputFile:
    """
    Reads archives and streams flattened table rows to Parquet or CSV.
    Uses `pyarrow` for tabular output.
    Runs in an isolated process to ensure that memory is released after execution.
    """
    require_pyarrow()

    activity_type = activity.info().activity_type

    # TODO: add the temporal context to the subprocess for logging
    # and propagating failure and cancellation policy
    with ProcessPoolExecutor(
        max_workers=1,
        initializer=worker_process_initializer,
        mp_context=multiprocessing.get_context('spawn'),
    ) as executor:
        future = executor.submit(
            _read_archives_and_write_output_tabular,
            data,
            activity_type,
        )
        return future.result()


@activity.defn
async def write_metadata_file(data: WriteMetadataFileInput) -> MetadataFile:
    """Create a metadata.json file in the artifact subdirectory"""
    artifact_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    metadata_file_path = artifact_subdirectory / f'{METADATA_FILE_NAME}.json'
    metadata_dict = {
        'note': 'This metadata file contains information about the exported dataset '
        'and the conditions under which it was generated.',
        'data': data.metadata.model_dump(),
        'schema': data.metadata.model_json_schema(),
    }
    with open(metadata_file_path, 'w', encoding='utf-8') as metafile:
        json.dump(metadata_dict, metafile, indent=2)

    return MetadataFile(
        file_path=metadata_file_path.as_posix(),
        file_size=metadata_file_path.stat().st_size,
    )


@activity.defn
async def export_dataset_to_upload(data: ExportDatasetInput) -> str:
    """
    Activity to export the generated dataset files to the specified upload.
    Creates a ZIP archive if `data.zip_output` is `True`.

    Returns:
        str: Relative path, within the upload raw directory, of the directory
            containing the exported dataset.
    """

    def unique_filename(filename: str, upload_files: StagingUploadFiles) -> str:
        """Generate a unique filename for the upload_files directory."""
        if not upload_files.raw_path_exists(filename):
            return filename

        filename_path = Path(filename)
        count = 1
        while True:
            _filename = filename_path.with_name(
                f'{filename_path.stem}({count}){filename_path.suffix}'
            ).as_posix()
            if not upload_files.raw_path_exists(_filename):
                return _filename
            count += 1

    upload_files = get_upload_files(data.upload_id, data.user_id)
    if not upload_files or not isinstance(upload_files, StagingUploadFiles):
        raise ValueError(
            f'Staging upload with ID {data.upload_id} for user {data.user_id} not found.'
        )
    exportable_dir_name = unique_filename(data.exportable_dir_name, upload_files)

    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )

    # Discover metadata, manifest, and data files in the artifacts subdirectory
    export_order = (METADATA_FILE_NAME, MANIFEST_FILE_NAME, DATA_FILE_NAME)
    files_by_stem = {
        path.stem: path
        for path in artifacts_subdirectory.iterdir()
        if path.is_file() and path.stem in export_order
    }
    exportable_filepaths = [
        files_by_stem[stem] for stem in export_order if stem in files_by_stem
    ]

    # Create a zip file containing all the source paths and the metadata file
    if data.zip_output:
        zippath = artifacts_subdirectory / f'{exportable_dir_name}.zip'
        with zipfile.ZipFile(zippath, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            for filepath in exportable_filepaths:
                zipf.write(filepath, arcname=filepath.name)
        # Add zip file to the NOMAD Upload
        upload_files.add_rawfiles(target_path=zippath.as_posix(), auto_decompress=False)
        return zippath.name

    # If not zipping, copy files to directory named exportable_dir_name
    exportable_dir_path = artifacts_subdirectory / exportable_dir_name
    exportable_dir_path.mkdir(exist_ok=True)
    for filepath in exportable_filepaths:
        temp_path = exportable_dir_path / filepath.name
        shutil.copy2(filepath, temp_path)
        # Add directory to the NOMAD Upload
        upload_files.add_rawfiles(
            target_path=exportable_dir_path.as_posix(), target_dir=exportable_dir_name
        )
    return exportable_dir_name


@activity.defn
async def cleanup_artifacts(data: CleanupArtifactsInput) -> None:
    """
    Activity to clean up the action artifacts directory.

    Args:
        data (CleanupArtifactsInput): Input data for cleaning up artifacts.
    """
    activity_logger = None
    try:
        info = activity.info()
        activity_logger = logger.bind(activity_type=info.activity_type)
        artifacts_subdirectory = Path(
            action_instance_artifacts_dir(data.export_entries_workflow_id)
        )
        if artifacts_subdirectory.exists():
            shutil.rmtree(artifacts_subdirectory)
    except Exception as e:
        if activity_logger is not None:
            activity_logger.error('error cleaning up artifacts', exc_info=e)
