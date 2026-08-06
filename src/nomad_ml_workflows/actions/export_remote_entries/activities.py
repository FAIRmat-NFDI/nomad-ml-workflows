import zipfile
from pathlib import Path

import boto3
from nomad.actions.manager import action_instance_artifacts_dir
from nomad.utils import get_logger
from temporalio import activity

from nomad_ml_workflows.actions.export_entries.activities import (
    DATA_FILE_NAME,
    MANIFEST_FILE_NAME,
    METADATA_FILE_NAME,
)
from nomad_ml_workflows.actions.export_remote_entries.models import (
    ExportRemoteDatasetInput,
    S3StorageSettings,
)

logger = get_logger(__name__)


def _upload_dataset_to_s3(
    data: ExportRemoteDatasetInput,
    storage_settings: S3StorageSettings,
    exportable_filepaths: list[Path],
    artifacts_subdirectory: Path,
) -> str:
    """Upload exported dataset files to S3-compatible remote storage."""
    client_kwargs = {}
    if storage_settings.endpoint_url:
        client_kwargs['endpoint_url'] = storage_settings.endpoint_url
    if storage_settings.region:
        client_kwargs['region_name'] = storage_settings.region
    if storage_settings.access_key_id:
        client_kwargs['aws_access_key_id'] = (
            storage_settings.access_key_id.get_secret_value()
        )
    if storage_settings.secret_access_key:
        client_kwargs['aws_secret_access_key'] = (
            storage_settings.secret_access_key.get_secret_value()
        )
    if storage_settings.session_token:
        client_kwargs['aws_session_token'] = (
            storage_settings.session_token.get_secret_value()
        )

    s3_client = boto3.client('s3', **client_kwargs)
    bucket = storage_settings.bucket
    prefix = storage_settings.prefix.strip('/')

    if data.zip_output:
        zippath = artifacts_subdirectory / f'{data.exportable_dir_name}.zip'
        with zipfile.ZipFile(zippath, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            for filepath in exportable_filepaths:
                zipf.write(filepath, arcname=filepath.name)

        object_key = (
            f'{prefix}/{data.exportable_dir_name}.zip'
            if prefix
            else f'{data.exportable_dir_name}.zip'
        )
        s3_client.upload_file(zippath.as_posix(), bucket, object_key)
        return f's3://{bucket}/{object_key}'

    base_key_prefix = (
        f'{prefix}/{data.exportable_dir_name}' if prefix else data.exportable_dir_name
    )
    for filepath in exportable_filepaths:
        object_key = f'{base_key_prefix}/{filepath.name}'
        s3_client.upload_file(filepath.as_posix(), bucket, object_key)

    return f's3://{bucket}/{base_key_prefix}/'


@activity.defn
async def upload_dataset_to_remote_storage(
    data: ExportRemoteDatasetInput,
) -> str:
    """
    Activity to upload the generated dataset files to remote storage.

    Returns:
        str: Remote URI where dataset files are stored (e.g. s3://bucket/prefix/file.zip).
    """
    artifacts_subdirectory = Path(
        action_instance_artifacts_dir(data.export_entries_workflow_id)
    )

    export_order = (METADATA_FILE_NAME, MANIFEST_FILE_NAME, DATA_FILE_NAME)
    files_by_stem = {
        path.stem: path
        for path in artifacts_subdirectory.iterdir()
        if path.is_file() and path.stem in export_order
    }
    exportable_filepaths = [
        files_by_stem[stem] for stem in export_order if stem in files_by_stem
    ]

    storage_settings = data.storage_settings
    if (
        isinstance(storage_settings, S3StorageSettings)
        or getattr(storage_settings, 'storage_type', None) == 's3'
    ):
        return _upload_dataset_to_s3(
            data, storage_settings, exportable_filepaths, artifacts_subdirectory
        )

    raise ValueError(
        f'Unsupported storage protocol: {getattr(storage_settings, "storage_type", type(storage_settings))}'
    )
