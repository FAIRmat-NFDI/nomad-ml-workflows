import json
import os
import shutil
import zipfile
from datetime import datetime, timezone
from math import ceil

from nomad.actions.manager import action_artifacts_dir, get_upload_files
from nomad.app.v1.models.models import MetadataPagination, MetadataRequired
from nomad.files import StagingUploadFiles
from nomad.search import search as nomad_search
from temporalio import activity

from nomad_ml_workflows.actions.export_entries.models import (
    CleanupArtifactsInput,
    CollectCursorsInput,
    CollectCursorsOutput,
    CreateArtifactSubdirectoryInput,
    Entry,
    ExportDatasetInput,
    MergeOutputFilesInput,
    SearchInput,
    SearchOutput,
)
from nomad_ml_workflows.actions.export_entries.utils import (
    merge_files,
    read_archive_entries,
    write_json_file,
    write_parquet_file,
)


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
async def search(data: SearchInput) -> SearchOutput:
    """
    Activity to perform NOMAD search based on the provided input data. The search
    results are written to a file in the specified format (Parquet or JSON) in the
    artifacts directory.

    Args:
        data (SearchInput): Input data for the search activity.

    Returns:
        SearchOutput: Output data from the search activity.
    """

    write_dataset_file = {
        'parquet': write_parquet_file,
        'json': write_json_file,
    }.get(data.batch_file_type)
    if write_dataset_file is None:
        raise ValueError(f'Unsupported batch file type "{data.batch_file_type}". ')

    if data.archive_required is not None:
        # When archives are to be accessed for certain required, then only
        # entry_id/upload_id are needed from ES.
        es_required = MetadataRequired(include=['entry_id', 'upload_id'])
    else:
        es_required = data.es_required

    start = datetime.now(timezone.utc).isoformat()
    response = nomad_search(
        user_id=data.user_id,
        owner=data.owner,
        query=data.query,
        required=es_required,
        pagination=data.pagination,
        aggregations={},
    )
    # Limit the number of exported entries
    entries_to_export = response.data[: data.max_entries_export_limit]

    if data.archive_required is not None:
        # build list[Entry] and populate the Entry.archive using read_archive_entries
        entry_list = [
            Entry(
                entry_id=entry['entry_id'],
                upload_id=entry['upload_id'],
            )
            for entry in entries_to_export
        ]
        read_archive_entries(entry_list, data.archive_required, data.user_id)
    else:
        entry_list = []
        for entry in entries_to_export:
            archive = {}
            for k in ['results', 'data', 'metadata', 'run', 'workflow2', 'workflow']:
                if k in entry:
                    archive[k] = entry[k]
            entry_list.append(
                Entry(
                    entry_id=entry['entry_id'],
                    archive=archive if archive else None,
                )
            )
    end = datetime.now(timezone.utc).isoformat()

    if entry_list:
        # convert the entry list into dicts
        entries_l_of_d = []
        for entry in entry_list:
            entry.upload_id = None
            entries_l_of_d.append(entry.model_dump(exclude_none=True))
        write_dataset_file(path=data.output_file_path, data=entries_l_of_d)

    return SearchOutput(
        search_start_time=start,
        search_end_time=end,
        num_entries_exported=len(entry_list),
    )


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
        data.artifact_subdirectory, 'data.' + data.output_file_type
    )

    merge_files(data.generated_file_paths, data.output_file_type, merged_file_path)

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

    exportable_filepaths = data.source_paths + [metadata_path]
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
        upload_files.add_rawfiles(path=zippath, auto_decompress=False)
        return zipname

    # If not zipping, copy files to directory named exportable_dir_name
    exportable_dir_path = os.path.join(data.artifact_subdirectory, exportable_dir_name)
    os.mkdir(exportable_dir_path)
    for filepath in exportable_filepaths:
        temp_path = os.path.join(exportable_dir_path, os.path.basename(filepath))
        shutil.copy2(filepath, temp_path)
        # Add directory to the NOMAD Upload
        upload_files.add_rawfiles(
            path=exportable_dir_path, target_dir=exportable_dir_name
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
