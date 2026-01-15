import os

from temporalio import activity

from nomad_actions.actions.entries.models import (
    CleanupArtifactsInput,
    CreateArtifactSubdirectoryInput,
    ExportDatasetInput,
    MergeOutputFilesInput,
    SearchInput,
    SearchOutput,
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
    from nomad.actions.manager import action_artifacts_dir

    subdir_path = os.path.join(action_artifacts_dir(), data.subdir_name)

    assert not os.path.exists(subdir_path)
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
    from datetime import datetime, timezone

    from nomad.search import search as nomad_search

    from nomad_actions.actions.entries.utils import (
        write_csv_file,
        write_json_file,
        write_parquet_file,
    )

    logger = activity.logger

    output_file_extension = os.path.splitext(data.output_file_path)[-1]
    if output_file_extension == '.parquet':
        write_dataset_file = write_parquet_file
    elif output_file_extension == '.csv':
        write_dataset_file = write_csv_file
    elif output_file_extension == '.json':
        write_dataset_file = write_json_file
    else:
        raise ValueError(
            f'Unsupported file format "{output_file_extension}". Please use .parquet, '
            '.csv, or .json extensions.'
        )

    output = SearchOutput()
    output.search_start_time = datetime.now(timezone.utc).isoformat()
    response = nomad_search(
        user_id=data.user_id,
        owner=data.owner,
        query=data.query,
        required=data.required,
        pagination=data.pagination,
        aggregations={},  # aggregations support can be added later
    )
    output.search_end_time = datetime.now(timezone.utc).isoformat()
    output.num_entries = len(response.data)

    if output.num_entries > 0:
        # skip writing empty files
        write_dataset_file(path=data.output_file_path, data=response.data)

    if response.pagination and response.pagination.next_page_after_value:
        output.pagination_next_page_after_value = (
            response.pagination.next_page_after_value
        )

    logger.info(
        f'Page {response.pagination.page} containing {len(response.data)} results '
        f'written to output file {data.output_file_path}.'
    )

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
    from nomad_actions.actions.entries.utils import merge_files

    if not data.generated_file_paths:
        return

    merged_file_path = os.path.join(
        data.artifact_subdirectory, '1.' + data.output_file_type
    )

    merge_files(data.generated_file_paths, merged_file_path)

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
    import json
    import zipfile

    from nomad.actions.manager import get_upload_files
    from nomad.files import StagingUploadFiles

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

    upload_files = get_upload_files(
        data.metadata.user_input.upload_id, data.metadata.user_input.user_id
    )
    if not upload_files:
        raise ValueError(
            f'Upload with ID {data.metadata.user_input.upload_id} for user '
            f'{data.metadata.user_input.user_id} not found.'
        )

    zipname = 'exported_entries_' + data.metadata.search_start_time + '.zip'
    zipname = unique_filename(zipname, upload_files)

    # Create a zip file containing all the source paths and the metadata file
    zippath = os.path.join(os.path.dirname(data.artifact_subdirectory), zipname)
    with zipfile.ZipFile(zippath, 'w') as zipf:
        for filepath in data.source_paths:
            arcname = os.path.basename(filepath)
            zipf.write(filepath, arcname=arcname)
        metadata_dict = data.metadata.model_dump()
        with zipf.open('metadata.json', 'w') as metafile:
            metafile.write(json.dumps(metadata_dict, indent=4).encode('utf-8'))

    # Upload zip file to the upload_files directory
    upload_files.add_rawfiles(path=zippath, auto_decompress=False)

    return zipname


@activity.defn
async def cleanup_artifacts(data: CleanupArtifactsInput) -> None:
    """
    Activity to clean up the action artifacts directory.

    Args:
        data (CleanupArtifactsInput): Input data for cleaning up artifacts.
    """
    import shutil

    if os.path.exists(data.subdir_path):
        shutil.rmtree(data.subdir_path)
