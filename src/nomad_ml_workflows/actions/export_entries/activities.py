import json
import multiprocessing
import shutil
import zipfile
from collections.abc import Iterator
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
    TableRowsFileOutput,
    WriteMetadataFileInput,
    WriteTabularFileInput,
)
from nomad_ml_workflows.actions.export_entries.utils import (
    generate_archives,
    write_dicts_to_json,
    write_table_rows_to_ndjson,
    write_table_rows_to_tabular_file,
)

config = nomad_config.get_plugin_entry_point(
    'nomad_ml_workflows.actions:export_entries'
)
logger = get_logger(__name__)

DATA_ARTIFACT_NAME = 'data'
MANIFEST_FILE_NAME = 'selected_entries'
METADATA_FILE_NAME = 'metadata'
TABLE_ROWS_FILE_NAME = 'table_rows.tmp.ndjson'
TABLE_SCHEMA_FILE_NAME = 'table_schema.tmp.arrow'


def _artifact_size(path: Path) -> int:
    """Return the aggregate byte size of a file or dataset directory."""
    if path.is_file():
        return path.stat().st_size
    return sum(child.stat().st_size for child in path.rglob('*') if child.is_file())


def _discover_exportable_artifacts(artifacts_directory: Path) -> list[Path]:
    """Find metadata, manifest, and data artifacts in their public order."""
    export_order = (METADATA_FILE_NAME, MANIFEST_FILE_NAME, DATA_ARTIFACT_NAME)
    artifacts_by_stem = {
        path.stem: path
        for path in artifacts_directory.iterdir()
        if (path.is_file() or path.is_dir()) and path.stem in export_order
    }
    return [
        artifacts_by_stem[stem] for stem in export_order if stem in artifacts_by_stem
    ]


def _iter_artifact_files(
    artifacts: list[Path],
) -> Iterator[tuple[Path, Path]]:
    """Yield files and paths relative to the exported dataset root."""
    for artifact in artifacts:
        if artifact.is_file():
            yield artifact, Path(artifact.name)
            continue
        for file_path in sorted(path for path in artifact.rglob('*') if path.is_file()):
            yield file_path, Path(artifact.name) / file_path.relative_to(artifact)


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
    output_file_path = artifacts_subdirectory / f'{DATA_ARTIFACT_NAME}.json'
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


@activity.defn
def read_archives_and_write_table_rows(
    data: ReadArchivesWorkflowInput,
) -> TableRowsFileOutput:
    """
    Read archives and write flattened NDJSON rows plus their Arrow schema sidecar.
    """
    activity_type = activity.info().activity_type

    activity_logger = logger.bind(activity_type=activity_type)
    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    manifest_file_path = artifacts_subdirectory / f'{MANIFEST_FILE_NAME}.json'
    table_rows_file_path = artifacts_subdirectory / TABLE_ROWS_FILE_NAME
    schema_file_path = artifacts_subdirectory / TABLE_SCHEMA_FILE_NAME
    temporary_table_rows_file_path = (
        artifacts_subdirectory / 'table_rows.partial.ndjson'
    )
    temporary_schema_file_path = artifacts_subdirectory / 'table_schema.partial.arrow'

    # load manifest
    with open(manifest_file_path, encoding='utf-8') as f:
        manifest = [ManifestEntry(**entry) for entry in json.load(f)]

    activity_logger.info('Reading archives and building the schema...')
    write_table_rows_to_ndjson(
        manifest,
        data.required,
        data.user_id,
        temporary_table_rows_file_path,
        temporary_schema_file_path,
        activity_logger,
    )
    temporary_schema_file_path.replace(schema_file_path)
    temporary_table_rows_file_path.replace(table_rows_file_path)

    return TableRowsFileOutput(
        table_rows_file_path=table_rows_file_path.as_posix(),
        schema_file_path=schema_file_path.as_posix(),
    )


def _write_output_tabular(
    data: WriteTabularFileInput, activity_type: str
) -> OutputFile:
    activity_logger = logger.bind(activity_type=activity_type)
    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )
    output_file_path = (
        artifacts_subdirectory / f'{DATA_ARTIFACT_NAME}.{data.output_file_format}'
    )
    temporary_output_file_path = output_file_path.with_stem(
        f'{output_file_path.stem}.tmp'
    )

    activity_logger.info('Writing table rows to tabular artifact...')
    num_entries_exported = write_table_rows_to_tabular_file(
        Path(data.table_rows_file_path),
        temporary_output_file_path,
        Path(data.schema_file_path),
        max_buffer_bytes=config.max_write_buffer_size_bytes,  # type: ignore
        max_buffer_rows=config.max_write_buffer_size_rows,  # type: ignore
        logger=activity_logger,
    )
    temporary_output_file_path.replace(output_file_path)
    activity_logger.info(
        f'{num_entries_exported} table rows written to '
        f'"data.{data.output_file_format}" artifact.'
    )

    return OutputFile(
        file_path=output_file_path.as_posix(),
        file_size=_artifact_size(output_file_path),
        num_entries_exported=num_entries_exported,
    )


@activity.defn
def write_output_tabular(data: WriteTabularFileInput) -> OutputFile:
    """
    Stream temporary NDJSON rows and their schema to Parquet or CSV.
    Runs in an isolated process to ensure that memory is released after execution.
    """
    activity_type = activity.info().activity_type

    # TODO: add the temporal context to the subprocess for logging
    # and propagating failure and cancellation policy
    with ProcessPoolExecutor(
        max_workers=1,
        mp_context=multiprocessing.get_context('spawn'),
    ) as executor:
        future = executor.submit(
            _write_output_tabular,
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

    exportable_artifacts = _discover_exportable_artifacts(artifacts_subdirectory)

    # Create a zip file containing all the source paths and the metadata file
    if data.zip_output:
        zippath = artifacts_subdirectory / f'{exportable_dir_name}.zip'
        with zipfile.ZipFile(zippath, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            for filepath, relative_path in _iter_artifact_files(exportable_artifacts):
                zipf.write(filepath, arcname=relative_path.as_posix())
        # Add zip file to the NOMAD Upload
        upload_files.add_rawfiles(target_path=zippath.as_posix(), auto_decompress=False)
        return zippath.name

    # If not zipping, copy files to directory named exportable_dir_name
    exportable_dir_path = artifacts_subdirectory / exportable_dir_name
    exportable_dir_path.mkdir(exist_ok=True)
    for artifact in exportable_artifacts:
        destination_path = exportable_dir_path / artifact.name
        if artifact.is_dir():
            shutil.copytree(artifact, destination_path, dirs_exist_ok=True)
        else:
            shutil.copy2(artifact, destination_path)
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
