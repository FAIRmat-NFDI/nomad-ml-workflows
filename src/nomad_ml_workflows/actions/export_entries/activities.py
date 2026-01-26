import json
import os
import shutil
import zipfile
from datetime import datetime, timezone

from nomad.actions.manager import action_artifacts_dir, get_upload_files
from nomad.files import StagingUploadFiles
from nomad.search import search as nomad_search
from temporalio import activity

from nomad_ml_workflows.actions.export_entries.models import (
    CleanupArtifactsInput,
    CreateArtifactSubdirectoryInput,
    ExportDatasetInput,
    MergeOutputFilesInput,
    SearchInput,
    SearchOutput,
)
from nomad_ml_workflows.actions.export_entries.utils import (
    merge_files,
    write_csv_file,
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
    results are written to a file in the specified format (Parquet, CSV, or JSON) in the
    artifacts directory.

    Args:
        data (SearchInput): Input data for the search activity.

    Returns:
        SearchOutput: Output data from the search activity.
    """

    write_dataset_file = {
        'parquet': write_parquet_file,
        'csv': write_csv_file,
        'json': write_json_file,
    }.get(data.output_file_type)
    if write_dataset_file is None:
        raise ValueError(
            f'Unsupported file type "{data.output_file_type}". '
            'Please use parquet, csv, or json as file types.'
        )

    start = datetime.now(timezone.utc).isoformat()
    response = nomad_search(
        user_id=data.user_id,
        owner=data.owner,
        query=data.query,
        required=data.required,
        pagination=data.pagination,
        aggregations={},  # aggregations support can be added later
    )
    end = datetime.now(timezone.utc).isoformat()

    # Limit the number of exported entries
    if len(response.data) > data.max_entries_export_limit:
        entry_list = response.data[: data.max_entries_export_limit]
    else:
        entry_list = response.data

    output = SearchOutput(
        search_start_time=start,
        search_end_time=end,
        num_entries_exported=len(entry_list),
        num_entries_available=response.pagination.total,
        pagination_next_page_after_value=response.pagination.next_page_after_value,
    )

    if len(entry_list) == 0:
        # skip writing empty files and stop subsequent searches
        output.pagination_next_page_after_value = None
    else:
        write_dataset_file(path=data.output_file_path, data=entry_list)

    return output


@activity.defn
async def merge_output_files(data: MergeOutputFilesInput) -> str | None:
    """
    Activity to merge multiple Parquet, CSV, or JSON files into a single file.

    Args:
        data (MergeOutputFilesInput): Input data for merging files.

    Returns:
        str | None: Path of the merged output file, or None if no files were merged.
    """

    if not data.generated_file_paths:
        return

    merged_file_path = os.path.join(
        data.artifact_subdirectory, 'merged.' + data.output_file_type
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
