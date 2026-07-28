import json
import os
import shutil
import zipfile
from datetime import datetime, timezone
from math import ceil

from nomad.actions.manager import action_artifacts_dir, get_upload_files
from nomad.app.v1.models.models import MetadataPagination, MetadataRequired, User
from nomad.files import StagingUploadFiles
from nomad.search import search as nomad_search
from nomad.utils import get_logger
from temporalio import activity

from nomad_ml_workflows.actions.export_entries.models import (
    CleanupArtifactsInput,
    CollectCursorsInput,
    CollectCursorsOutput,
    CreateArtifactSubdirectoryInput,
    ExportDatasetInput,
    MergeOutputFilesInput,
    RenameGeneratedFileInput,
    SearchPageInput,
    SearchPageOutput,
)
from nomad_ml_workflows.actions.export_entries.utils import (
    merge_files,
    write_json_file,
    write_parquet_file,
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
async def collect_page_cursors(data: CollectCursorsInput) -> CollectCursorsOutput:
    """
    Activity to serially walk NOMAD search pagination and collect all
    page_after_value cursors needed for parallel page fetching.

    Only entry IDs are requested to minimise payload size.

    Assumption: The cursors are valid for any subsequent search with the same query,
    regardless of the required fields used in those searches.

    Args:
        data (CollectCursorsInput): Input data specifying the search and limits.

    Returns:
        CollectCursorsOutput: All page cursors and the total entry count.
    """

    # Use minimal required fields so the probe searches are as fast as possible.
    required = MetadataRequired(include=['entry_id'])

    # First page: cursor is None (start of results).
    pagination = MetadataPagination(page_size=data.page_size)
    response = nomad_search(
        user_id=data.user_id,
        owner=data.owner,
        query=data.query,
        required=required,
        pagination=pagination,
        aggregations={},
    )

    # determine the number of pages needed, incl. the first page
    num_entries_available = response.pagination.total
    num_entries_to_export = min(num_entries_available, data.max_entries_export_limit)
    num_pages = (
        ceil(num_entries_to_export / data.page_size) if num_entries_to_export > 0 else 0
    )

    if num_pages == 0:
        return CollectCursorsOutput(
            page_after_values=[],
            num_entries_available=num_entries_available,
            num_pages=num_pages,
        )

    # Collect the page_after_value cursor for each page.
    # The first page starts with a None cursor.
    page_after_values: list[str | None] = [None]
    cursor = response.pagination.next_page_after_value
    for _ in range(num_pages - 1):
        if cursor is None:
            break
        page_after_values.append(cursor)
        pagination = MetadataPagination(
            page_size=data.page_size, page_after_value=cursor
        )
        response = nomad_search(
            user_id=data.user_id,
            owner=data.owner,
            query=data.query,
            required=required,
            pagination=pagination,
            aggregations={},
        )
        cursor = response.pagination.next_page_after_value

    return CollectCursorsOutput(
        page_after_values=page_after_values,
        num_entries_available=num_entries_available,
        num_pages=num_pages,
    )


@activity.defn
async def read_archives(data: SearchPageInput) -> SearchPageOutput:
    """
    Activity to read archives of the searched entries. The required fields are read
    from the archives and written to a file in the specified format (Parquet or JSON)
    in the artifacts directory.

    Args:
        data (ReadArchivesInput): Input data for the search activity.

    Returns:
        SearchPageOutput: Output data from the search and read archives activity.
    """
    from nomad.app.v1.routers.entries import _read_entry_from_archive, _Uploads
    from nomad.archive.required import RequiredReader

    start = datetime.now(timezone.utc).isoformat()

    info = activity.info()

    activity_logger = logger.bind(
        activity_type=info.activity_type,
        search_page_num=data.page_num,
    )

    # Find entries whose archives are to be read
    response = nomad_search(
        user_id=data.user_id,
        owner=data.owner,
        query=data.query,
        required=MetadataRequired(include=['entry_id', 'upload_id']),
        pagination=data.pagination,
    )
    entries: list = [
        {
            'entry_id': entry['entry_id'],
            'upload_id': entry['upload_id'],
        }
        for entry in response.data
    ]
    entries = entries[: data.max_entries_export_limit]  #  Apply max limit

    # setup required reader
    required_reader = RequiredReader(
        data.required,
        resolve_inplace=True,
        user=User(user_id=data.user_id),
    )
    entry_archives = []

    # For each entry
    with _Uploads() as uploads:
        for entry in entries:
            try:
                entry_archive = _read_entry_from_archive(
                    entry, uploads, required_reader
                )
                entry_archives.append(entry_archive)
            except Exception as e:
                activity_logger.error(
                    'failed to read entry archive',
                    entry_id=entry['entry_id'],
                    exc_info=e,
                )

    end = datetime.now(timezone.utc).isoformat()

    write_dataset_file = {
        'parquet': write_parquet_file,
        'json': write_json_file,
    }.get(data.batch_file_format)
    if write_dataset_file is None:
        raise ValueError(f'Unsupported batch file format "{data.batch_file_format}". ')

    if entry_archives:
        num_entries_exported = write_dataset_file(
            path=data.output_file_path,
            data=entry_archives,
            logger=activity_logger,
        )
    else:
        num_entries_exported = 0

    activity_logger.info(
        f'exported {num_entries_exported}/{len(entry_archives)} entries'
    )

    return SearchPageOutput(
        search_start_time=start,
        search_end_time=end,
        num_entries_exported=num_entries_exported,
    )


@activity.defn
def rename_generated_file(data: RenameGeneratedFileInput) -> str | None:
    target_file_path = os.path.join(
        data.artifact_subdirectory, 'data.' + data.output_file_format
    )
    os.rename(data.generated_file_path, target_file_path)
    return target_file_path


@activity.defn
async def merge_output_files(data: MergeOutputFilesInput) -> str | None:
    """
    Activity to merge multiple batch files into a single file.

    Args:
        data (MergeOutputFilesInput): Input data for merging files.

    Returns:
        str | None: Path of the merged output file, or None if no files were merged.
    """

    if not data.generated_file_paths:
        raise ValueError('No generated file paths provided for merging.')

    merged_file_path = os.path.join(
        data.artifact_subdirectory, 'data.' + data.output_file_format
    )

    merge_files(data.generated_file_paths, data.output_file_format, merged_file_path)

    return merged_file_path


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
    if not upload_files:
        raise ValueError(
            f'Upload with ID {data.upload_id} for user {data.user_id} not found.'
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
    if data.source_path:
        exportable_filepaths.append(data.source_path)

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
