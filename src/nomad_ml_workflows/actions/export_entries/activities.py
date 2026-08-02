import json
import multiprocessing
import os
import shutil
import zipfile
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone

from nomad.actions.manager import action_artifacts_dir
from nomad.app.v1.models.models import MetadataPagination, MetadataRequired
from nomad.config import config as nomad_config
from nomad.files import StagingUploadFiles
from nomad.search import search as nomad_search
from nomad.uploads import get_upload_files
from nomad.utils import get_logger
from temporalio import activity

from nomad_ml_workflows.actions.export_entries.models import (
    CleanupArtifactsInput,
    CreateArtifactSubdirectoryInput,
    ExportDatasetInput,
    ManifestEntry,
    OutputFile,
    PrepapeManifestOutput,
    PrepareManifestInput,
    ReadArchivesWorkflowInput,
)
from nomad_ml_workflows.actions.export_entries.utils import (
    generate_archives,
    generate_table_rows,
    worker_process_initializer,
    write_dicts_to_json,
    write_table_rows_to_ndjson,
    write_table_rows_to_tabular_file,
)

config = nomad_config.get_plugin_entry_point(
    'nomad_ml_workflows.actions:export_entries'
)
logger = get_logger(__name__)


@activity.defn
async def create_artifact_subdirectory(data: CreateArtifactSubdirectoryInput) -> str:
    """
    Creates a subdirectory within the action artifacts directory.

    Args:
        data (CreateArtifactSubdirectoryInput): Input data for creating subdirectory.

    Returns:
        str: Path to the created subdirectory.
    """

    subdir_path = os.path.join(action_artifacts_dir(), data.subdir_name)

    assert not os.path.exists(subdir_path), (
        f'Artifact subdirectory "{subdir_path}" already exists.'
    )

    os.makedirs(subdir_path)

    return subdir_path


@activity.defn
def prepare_manifest(data: PrepareManifestInput) -> PrepapeManifestOutput:
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
    write_dicts_to_json(manifest, data.manifest_file_path)

    return PrepapeManifestOutput(
        search_start_time=starttime,
        search_end_time=endtime,
        num_entries_available=num_entries_available,
        num_entries_selected=num_entries_selected,
        reached_max_entries_limit=reached_max_entries_limit,
    )


@activity.defn
def read_archives_and_write_output_json(
    data: ReadArchivesWorkflowInput,
) -> OutputFile:
    """
    Reads the archives and writes the output JSON file.
    """
    output_file_path = f'{data.artifact_subdirectory}/data.{data.output_file_format}'

    # load manifest
    with open(data.manifest_file_path, encoding='utf-8') as f:
        manifest = [ManifestEntry(**entry) for entry in json.load(f)]

    info = activity.info()
    activity_logger = logger.bind(activity_type=info.activity_type)

    archives = generate_archives(manifest, data.required, data.user_id, activity_logger)
    num_entries_exported = write_dicts_to_json(archives, output_file_path)

    return OutputFile(
        file_path=output_file_path,
        file_size=os.path.getsize(output_file_path),
        num_entries_exported=num_entries_exported,
    )


def _read_archives_and_write_output_tabular(
    data: ReadArchivesWorkflowInput, activity_type: str
) -> OutputFile:
    activity_logger = logger.bind(activity_type=activity_type)

    table_rows_file_path = f'{data.artifact_subdirectory}/table_rows.tmp.ndjson'
    output_file_path = f'{data.artifact_subdirectory}/data.{data.output_file_format}'

    # load manifest
    with open(data.manifest_file_path, encoding='utf-8') as f:
        manifest = [ManifestEntry(**entry) for entry in json.load(f)]

    rows_with_columns_quantity_def = generate_table_rows(
        manifest, data.required, data.user_id, activity_logger
    )

    activity_logger.info('Reading archives and building the schema...')
    columns_quantity_def = write_table_rows_to_ndjson(
        rows_with_columns_quantity_def, table_rows_file_path
    )
    activity_logger.info('Writing table rows to tabular file...')
    num_entries_exported = write_table_rows_to_tabular_file(
        table_rows_file_path,
        output_file_path,
        columns_quantity_def,
        max_buffer_bytes=config.max_write_buffer_size_bytes,  # type: ignore
        logger=activity_logger,
    )
    activity_logger.info(
        f'{num_entries_exported} table rows written to '
        f'"data.{data.output_file_format}" file.'
    )

    return OutputFile(
        file_path=output_file_path,
        file_size=os.path.getsize(output_file_path),
        num_entries_exported=num_entries_exported,
    )


@activity.defn
def read_archives_and_write_output_tabular(
    data: ReadArchivesWorkflowInput,
) -> OutputFile:
    """
    Reads archives and streams flattened table rows to Parquet or CSV.
    Runs in an isolated process to ensure that memory is released after execution.
    """

    if data.output_file_format not in {'parquet', 'csv'}:
        raise ValueError(
            f'Unsupported tabular output format: {data.output_file_format}'
        )

    activity_type = activity.info().activity_type

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
async def export_dataset_to_upload(data: ExportDatasetInput) -> str:
    """
    Activity to export the generated dataset files as a zip file to the specified
    upload. A metadata file is also included in the zip.

    Args:
        data (ExportDatasetInput): Input data for exporting the dataset to the upload.
    Returns:
        str: Path to the saved zip file in the upload.
    """

    def unique_filename(filename: str, upload_files: StagingUploadFiles) -> str:
        """Generate a unique filename for the upload_files directory."""
        if not upload_files.raw_path_exists(filename):
            return filename

        count = 1
        while True:
            name, ext = os.path.splitext(filename)
            _filename = f'{name}({count}){ext}'
            if not upload_files.raw_path_exists(_filename):
                return _filename
            count += 1

    upload_files = get_upload_files(data.upload_id, data.user_id)
    if not upload_files or not isinstance(upload_files, StagingUploadFiles):
        raise ValueError(
            f'Staging upload with ID {data.upload_id} for user {data.user_id} not found.'
        )

    # Create a metadata.json file in the artifact subdirectory
    metadata_dict = {
        'note': 'This metadata file contains information about the exported dataset '
        'and the conditions under which it was generated.',
        'data': data.metadata.model_dump(),
        'schema': data.metadata.model_json_schema(),
    }
    metadata_path = os.path.join(data.artifact_subdirectory, 'metadata.json')
    with open(metadata_path, 'w', encoding='utf-8') as metafile:
        json.dump(metadata_dict, metafile, indent=4)

    exportable_filepaths = [metadata_path]
    if data.source_paths:
        exportable_filepaths.extend(data.source_paths)

    exportable_dir_name = unique_filename(data.exportable_dir_name, upload_files)

    # Create a zip file containing all the source paths and the metadata file
    if data.zip_output:
        zipname = exportable_dir_name + '.zip'
        zippath = os.path.join(data.artifact_subdirectory, zipname)
        with zipfile.ZipFile(zippath, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            for filepath in exportable_filepaths:
                arcname = os.path.basename(filepath)
                zipf.write(filepath, arcname=arcname)
        # Add zip file to the NOMAD Upload
        upload_files.add_rawfiles(target_path=zippath, auto_decompress=False)
        return zipname

    # If not zipping, copy files to directory named exportable_dir_name
    exportable_dir_path = os.path.join(data.artifact_subdirectory, exportable_dir_name)
    os.mkdir(exportable_dir_path)
    for filepath in exportable_filepaths:
        temp_path = os.path.join(exportable_dir_path, os.path.basename(filepath))
        shutil.copy2(filepath, temp_path)
        # Add directory to the NOMAD Upload
        upload_files.add_rawfiles(
            target_path=exportable_dir_path, target_dir=exportable_dir_name
        )
    return exportable_dir_name


@activity.defn
async def cleanup_artifacts(data: CleanupArtifactsInput) -> None:
    """
    Activity to clean up the action artifacts directory.

    Args:
        data (CleanupArtifactsInput): Input data for cleaning up artifacts.
    """

    if os.path.exists(data.subdir_path):
        shutil.rmtree(data.subdir_path)
