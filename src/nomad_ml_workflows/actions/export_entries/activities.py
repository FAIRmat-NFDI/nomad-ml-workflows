import json
import multiprocessing
import os
import shutil
import time
import traceback
import uuid
import zipfile
from collections.abc import Iterable
from datetime import datetime, timezone
from multiprocessing.connection import Connection
from typing import Any

from nomad.actions.manager import action_artifacts_dir
from nomad.app.v1.models.models import MetadataPagination, MetadataRequired
from nomad.config import config as nomad_config
from nomad.files import StagingUploadFiles
from nomad.search import search as nomad_search
from nomad.uploads import get_upload_files
from nomad.utils import get_logger
from temporalio import activity
from temporalio.exceptions import CancelledError

from nomad_ml_workflows.actions.export_entries.models import (
    CleanupArtifactsInput,
    CreateArtifactSubdirectoryInput,
    ExportDatasetInput,
    ManifestEntry,
    OutputFile,
    PrepareManifestInput,
    PrepareManifestOutput,
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

_SUBPROCESS_POLL_INTERVAL_SECONDS = 1
_SUBPROCESS_STOP_TIMEOUT_SECONDS = 5
_PROGRESS_REPORT_INTERVAL = 100


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
    write_dicts_to_json(manifest, data.manifest_file_path)

    return PrepareManifestOutput(
        search_start_time=starttime,
        search_end_time=endtime,
        num_entries_available=num_entries_available,
        num_entries_selected=num_entries_selected,
        reached_max_entries_limit=reached_max_entries_limit,
    )


def _with_subprocess_progress(
    items: Iterable[Any], connection: Connection, phase: str
) -> Iterable[Any]:
    count = 0
    connection.send(('progress', {'phase': phase, 'num_entries_processed': count}))
    for item in items:
        count += 1
        if count % _PROGRESS_REPORT_INTERVAL == 0:
            connection.send(
                ('progress', {'phase': phase, 'num_entries_processed': count})
            )
        yield item
    connection.send(('progress', {'phase': phase, 'num_entries_processed': count}))


def _read_archives_and_write_output_json(
    data: ReadArchivesWorkflowInput,
    activity_type: str,
    output_file_path: str,
    progress_connection: Connection,
) -> OutputFile:
    # load manifest
    with open(data.manifest_file_path, encoding='utf-8') as f:
        manifest = [ManifestEntry(**entry) for entry in json.load(f)]

    activity_logger = logger.bind(activity_type=activity_type)

    archives = generate_archives(manifest, data.required, data.user_id, activity_logger)
    num_entries_exported = write_dicts_to_json(
        _with_subprocess_progress(
            archives,
            progress_connection,
            'reading_archives',
        ),
        output_file_path,
    )

    return OutputFile(
        file_path=output_file_path,
        file_size=os.path.getsize(output_file_path),
        num_entries_exported=num_entries_exported,
    )


def _read_archives_and_write_output_tabular(
    data: ReadArchivesWorkflowInput,
    activity_type: str,
    output_file_path: str,
    table_rows_file_path: str,
    progress_connection: Connection,
) -> OutputFile:
    activity_logger = logger.bind(activity_type=activity_type)

    # load manifest
    with open(data.manifest_file_path, encoding='utf-8') as f:
        manifest = [ManifestEntry(**entry) for entry in json.load(f)]

    rows_with_columns_quantity_def = generate_table_rows(
        manifest, data.required, data.user_id, activity_logger
    )

    activity_logger.info('Reading archives and building the schema...')
    columns_quantity_def = write_table_rows_to_ndjson(
        _with_subprocess_progress(
            rows_with_columns_quantity_def,
            progress_connection,
            'reading_archives',
        ),
        table_rows_file_path,
    )
    activity_logger.info('Writing table rows to tabular file...')
    progress_connection.send(('progress', {'phase': 'writing_output'}))
    num_entries_exported = write_table_rows_to_tabular_file(
        table_rows_file_path,
        output_file_path,
        columns_quantity_def,
        max_buffer_bytes=config.max_write_buffer_size_bytes,  # type: ignore
        logger=activity_logger,
    )
    progress_connection.send(
        (
            'progress',
            {
                'phase': 'writing_output',
                'num_entries_processed': num_entries_exported,
            },
        )
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


def _read_archives_subprocess(
    data: ReadArchivesWorkflowInput,
    activity_type: str,
    output_file_path: str,
    table_rows_file_path: str,
    connection: Connection,
) -> None:
    try:
        worker_process_initializer()
        if data.output_file_format == 'json':
            output = _read_archives_and_write_output_json(
                data,
                activity_type,
                output_file_path,
                connection,
            )
        else:
            output = _read_archives_and_write_output_tabular(
                data,
                activity_type,
                output_file_path,
                table_rows_file_path,
                connection,
            )
        connection.send(('result', output.model_dump()))
    except BaseException:
        connection.send(('error', traceback.format_exc()))
    finally:
        connection.close()


def _drain_subprocess_messages(
    connection: Connection,
    progress: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    result = None
    error = None
    while connection.poll():
        try:
            message_type, payload = connection.recv()
        except EOFError:
            break
        if message_type == 'progress':
            progress.update(payload)
        elif message_type == 'result':
            result = payload
        elif message_type == 'error':
            error = payload
    return result, error


def _stop_subprocess(process) -> None:
    if not process.is_alive():
        process.join()
        return

    process.terminate()
    process.join(timeout=_SUBPROCESS_STOP_TIMEOUT_SECONDS)
    if process.is_alive():
        process.kill()
        process.join()


def _wait_for_subprocess(
    process,
    connection: Connection,
    start_to_close_timeout_seconds: float | None,
) -> OutputFile:
    started_at = time.monotonic()
    progress: dict[str, Any] = {
        'phase': 'starting_subprocess',
        'num_entries_processed': 0,
    }
    result = None
    error = None

    try:
        while process.is_alive():
            poll_interval = _SUBPROCESS_POLL_INTERVAL_SECONDS
            if start_to_close_timeout_seconds is not None:
                remaining_timeout = start_to_close_timeout_seconds - (
                    time.monotonic() - started_at
                )
                if remaining_timeout <= 0:
                    raise TimeoutError(
                        'Archive export subprocess exceeded its start-to-close timeout.'
                    )
                poll_interval = min(poll_interval, remaining_timeout)

            process.join(timeout=poll_interval)
            new_result, new_error = _drain_subprocess_messages(connection, progress)
            result = new_result or result
            error = new_error or error
            activity.heartbeat(progress)

            if activity.is_cancelled():
                raise CancelledError('Archive export activity was cancelled.')
            if (
                start_to_close_timeout_seconds is not None
                and time.monotonic() - started_at >= start_to_close_timeout_seconds
            ):
                raise TimeoutError(
                    'Archive export subprocess exceeded its start-to-close timeout.'
                )

        process.join()
        new_result, new_error = _drain_subprocess_messages(connection, progress)
        result = new_result or result
        error = new_error or error
        activity.heartbeat(progress)

        if activity.is_cancelled():
            raise CancelledError('Archive export activity was cancelled.')
        if error is not None:
            raise RuntimeError(f'Archive export subprocess failed:\n{error}')
        if process.exitcode != 0:
            raise RuntimeError(
                f'Archive export subprocess exited with code {process.exitcode}.'
            )
        if result is None:
            raise RuntimeError('Archive export subprocess returned no result.')
        return OutputFile.model_validate(result)
    except BaseException:
        _stop_subprocess(process)
        raise


def _temporary_output_path(output_file_path: str, token: str) -> str:
    base_path, extension = os.path.splitext(output_file_path)
    return f'{base_path}.{token}.tmp{extension}'


def _remove_temporary_file(file_path: str) -> None:
    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass


def _run_read_archives_activity(data: ReadArchivesWorkflowInput) -> OutputFile:
    activity_started_at = time.monotonic()
    activity_info = activity.info()
    output_file_path = f'{data.artifact_subdirectory}/data.{data.output_file_format}'
    temporary_token = uuid.uuid4().hex
    temporary_output_file_path = _temporary_output_path(
        output_file_path, temporary_token
    )
    temporary_table_rows_file_path = os.path.join(
        data.artifact_subdirectory,
        f'table_rows.{temporary_token}.tmp.ndjson',
    )

    context = multiprocessing.get_context('spawn')
    parent_connection, child_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_read_archives_subprocess,
        args=(
            data,
            activity_info.activity_type,
            temporary_output_file_path,
            temporary_table_rows_file_path,
            child_connection,
        ),
    )
    process_started = False

    try:
        process.start()
        process_started = True
        child_connection.close()

        timeout = activity_info.start_to_close_timeout
        remaining_timeout_seconds = None
        if timeout is not None:
            remaining_timeout_seconds = max(
                0,
                timeout.total_seconds() - (time.monotonic() - activity_started_at),
            )
        output = _wait_for_subprocess(
            process,
            parent_connection,
            remaining_timeout_seconds,
        )
        os.replace(temporary_output_file_path, output_file_path)
        return OutputFile(
            file_path=output_file_path,
            file_size=output.file_size,
            num_entries_exported=output.num_entries_exported,
        )
    finally:
        if process_started and process.is_alive():
            _stop_subprocess(process)
        child_connection.close()
        parent_connection.close()
        _remove_temporary_file(temporary_output_file_path)
        _remove_temporary_file(temporary_table_rows_file_path)


@activity.defn(no_thread_cancel_exception=True)
def read_archives_and_write_output_json(
    data: ReadArchivesWorkflowInput,
) -> OutputFile:
    """Read archives and write JSON in a cancellation-aware subprocess."""

    if data.output_file_format != 'json':
        raise ValueError('Unsupported JSON output format.')

    return _run_read_archives_activity(data)


@activity.defn(no_thread_cancel_exception=True)
def read_archives_and_write_output_tabular(
    data: ReadArchivesWorkflowInput,
) -> OutputFile:
    """Read archives and write tabular output in a cancellation-aware subprocess."""

    if data.output_file_format not in {'parquet', 'csv'}:
        raise ValueError(
            f'Unsupported tabular output format: {data.output_file_format}'
        )

    return _run_read_archives_activity(data)


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
