"""
Activities for uploading exported dataset files to remote storage providers.
"""

import json
import zipfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from urllib.parse import urlparse

from nomad.actions.manager import action_instance_artifacts_dir
from nomad.files import StagingUploadFiles
from nomad.uploads import get_upload_files
from nomad.utils import get_logger
from temporalio import activity

from nomad_ml_workflows.actions.export_entries.activities import (
    DATA_ARTIFACT_NAME,
    MANIFEST_FILE_NAME,
    METADATA_FILE_NAME,
)
from nomad_ml_workflows.actions.export_entries.utils import (
    discover_exportable_artifacts,
    iter_artifact_files,
)
from nomad_ml_workflows.actions.export_remote_entries.models import (
    CopyRemoteDatasetToUploadInput,
    ExportRemoteDatasetInput,
    ResolveExportRemoteEntriesRuntimeOutput,
    S3StorageSettings,
)

logger = get_logger(__name__)


@activity.defn
def resolve_export_remote_entries_runtime_activity() -> (
    ResolveExportRemoteEntriesRuntimeOutput
):
    """Resolve runtime settings and S3 storage settings from entry point config and environment."""
    from nomad_ml_workflows.actions.export_remote_entries import (
        current_export_remote_entries_action_entry_point,
    )

    entry_point = current_export_remote_entries_action_entry_point()
    resolved_storage_settings = None
    if entry_point.s3_mode == 'env':
        resolved_storage_settings = entry_point.resolve_s3_storage_settings()

    return ResolveExportRemoteEntriesRuntimeOutput(
        s3_mode=entry_point.s3_mode,
        resolved_storage_settings=resolved_storage_settings,
    )


def _build_boto3_client_kwargs(storage_settings: S3StorageSettings) -> dict[str, Any]:
    """Construct boto3 S3 client keyword arguments from storage settings."""
    client_kwargs: dict[str, Any] = {}
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
    return client_kwargs


def _build_s3_key(prefix: str, *parts: str) -> str:
    """Build a clean S3 object key with optional prefix."""
    clean_prefix = prefix.strip('/')
    subpath = '/'.join(p.strip('/') for p in parts if p)
    return f'{clean_prefix}/{subpath}' if clean_prefix else subpath


def _get_staging_upload_files(user_id: str, upload_id: str) -> StagingUploadFiles:
    """Load and validate the destination staging upload."""
    upload_files = get_upload_files(upload_id, user_id)
    if not upload_files or not isinstance(upload_files, StagingUploadFiles):
        raise ValueError(
            f'Staging upload with ID {upload_id} for user {user_id} not found.'
        )
    return upload_files


def _unique_upload_filename(filename: str, upload_files: StagingUploadFiles) -> str:
    """Generate a collision-safe filename for a staging upload."""
    if not upload_files.raw_path_exists(filename):
        return filename

    filename_path = Path(filename)
    count = 1
    while True:
        candidate = filename_path.with_name(
            f'{filename_path.stem}({count}){filename_path.suffix}'
        ).as_posix()
        if not upload_files.raw_path_exists(candidate):
            return candidate
        count += 1


DEFAULT_PRESIGNED_URL_EXPIRATION_SECONDS = 2 * 24 * 3600  # 2 days (172800s)


def _parse_s3_uri(
    remote_uri: str, default_bucket: str | None = None
) -> tuple[str, str]:
    """Parse an S3 URI or presigned URL into its bucket and object key/prefix."""
    parsed_uri = urlparse(remote_uri)
    if parsed_uri.scheme == 's3':
        key = parsed_uri.path.lstrip('/')
        if not parsed_uri.netloc or not key:
            raise ValueError(f'Invalid S3 URI: {remote_uri}')
        return parsed_uri.netloc, key
    elif parsed_uri.scheme in ('http', 'https'):
        path = unquote(parsed_uri.path.lstrip('/'))
        if default_bucket and path.startswith(f'{default_bucket}/'):
            key = path[len(default_bucket) + 1 :]
            return default_bucket, key
        elif default_bucket:
            return default_bucket, path
        parts = path.split('/', 1)
        if len(parts) > 1:
            return parts[0], parts[1]
        elif parts and parts[0]:
            return parsed_uri.netloc, parts[0]
        raise ValueError(f'Could not determine S3 bucket/key from URL: {remote_uri}')
    raise ValueError(f'Invalid S3 URI: {remote_uri}')


def _upload_dataset_to_s3(
    data: ExportRemoteDatasetInput,
    storage_settings: S3StorageSettings,
    exportable_artifacts: list[Path],
    artifacts_subdirectory: Path,
) -> str:
    """Upload exported dataset files to S3-compatible remote storage and return a presigned download URL."""
    import boto3

    client_kwargs = _build_boto3_client_kwargs(storage_settings)
    s3_client = boto3.client('s3', **client_kwargs)
    bucket = storage_settings.bucket
    prefix = storage_settings.prefix

    if data.zip_output:
        zippath = artifacts_subdirectory / f'{data.exportable_dir_name}.zip'
        with zipfile.ZipFile(zippath, 'w', compression=zipfile.ZIP_DEFLATED) as zipf:
            for filepath, relative_path in iter_artifact_files(exportable_artifacts):
                zipf.write(filepath, arcname=relative_path.as_posix())

        object_key = _build_s3_key(prefix, f'{data.exportable_dir_name}.zip')
        s3_client.upload_file(zippath.as_posix(), bucket, object_key)
        try:
            return s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': object_key},
                ExpiresIn=DEFAULT_PRESIGNED_URL_EXPIRATION_SECONDS,
            )
        except Exception:
            return f's3://{bucket}/{object_key}'

    base_key_prefix = _build_s3_key(prefix, data.exportable_dir_name)
    primary_object_key = None
    for filepath, relative_path in iter_artifact_files(exportable_artifacts):
        object_key = f'{base_key_prefix}/{relative_path.as_posix()}'
        s3_client.upload_file(filepath.as_posix(), bucket, object_key)
        if filepath.stem == DATA_ARTIFACT_NAME or primary_object_key is None:
            primary_object_key = object_key

    if primary_object_key:
        try:
            return s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket, 'Key': primary_object_key},
                ExpiresIn=DEFAULT_PRESIGNED_URL_EXPIRATION_SECONDS,
            )
        except Exception:
            pass

    return f's3://{bucket}/{base_key_prefix}/'


def _copy_dataset_from_s3_to_upload(
    data: CopyRemoteDatasetToUploadInput,
    storage_settings: S3StorageSettings,
) -> str:
    """Download an S3 dataset and add it to a NOMAD staging upload."""
    import boto3

    bucket, key = _parse_s3_uri(data.remote_uri, default_bucket=storage_settings.bucket)
    client_kwargs = _build_boto3_client_kwargs(storage_settings)
    s3_client = boto3.client('s3', **client_kwargs)
    upload_files = _get_staging_upload_files(data.user_id, data.upload_id)

    with TemporaryDirectory(prefix='nomad-remote-export-') as temporary_directory:
        temporary_directory_path = Path(temporary_directory)

        if data.zip_output:
            filename = _unique_upload_filename(Path(key).name, upload_files)
            local_path = temporary_directory_path / filename
            s3_client.download_file(bucket, key, local_path.as_posix())
            upload_files.add_rawfiles(
                target_path=local_path.as_posix(), auto_decompress=False
            )
            return filename

        prefix = key.rstrip('/') + '/'
        directory_name = _unique_upload_filename(
            Path(key.rstrip('/')).name, upload_files
        )
        local_directory = temporary_directory_path / directory_name
        downloaded_files = 0
        continuation_token: str | None = None

        while True:
            list_kwargs: dict[str, Any] = {
                'Bucket': bucket,
                'Prefix': prefix,
            }
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token
            response = s3_client.list_objects_v2(**list_kwargs)

            for item in response.get('Contents', []):
                object_key = item.get('Key')
                if not object_key or object_key.endswith('/'):
                    continue
                relative_key = object_key.removeprefix(prefix)
                relative_path = Path(relative_key)
                if (
                    not relative_key
                    or relative_path.is_absolute()
                    or '..' in relative_path.parts
                ):
                    raise ValueError(
                        f'Invalid object key under S3 prefix: {object_key}'
                    )

                local_path = local_directory / relative_path
                local_path.parent.mkdir(parents=True, exist_ok=True)
                s3_client.download_file(bucket, object_key, local_path.as_posix())
                downloaded_files += 1

            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')
            if not continuation_token:
                raise ValueError(
                    'S3 object listing was truncated without a continuation token.'
                )

        if downloaded_files == 0:
            raise ValueError(f'No objects found under S3 prefix: {data.remote_uri}')

        upload_files.add_rawfiles(
            target_path=local_directory.as_posix(), target_dir=directory_name
        )
        return directory_name


@activity.defn
def read_num_entries_exported(export_entries_workflow_id: str) -> int:
    """Read the exported-entry count from the generated metadata file."""
    artifacts_dir = Path(action_instance_artifacts_dir(export_entries_workflow_id))
    metadata_file_path = artifacts_dir / f'{METADATA_FILE_NAME}.json'

    with open(metadata_file_path, encoding='utf-8') as metadata_file:
        metadata = json.load(metadata_file)

    try:
        num_entries_exported = metadata['data']['num_entries_exported']
    except (KeyError, TypeError) as exc:
        raise ValueError(
            f'Metadata file {metadata_file_path} does not contain '
            'data.num_entries_exported.'
        ) from exc

    if not isinstance(num_entries_exported, int) or isinstance(
        num_entries_exported, bool
    ):
        raise ValueError(
            f'Metadata file {metadata_file_path} contains an invalid '
            'data.num_entries_exported value.'
        )

    return num_entries_exported


@activity.defn
async def upload_dataset_to_remote_storage(
    data: ExportRemoteDatasetInput,
) -> str:
    """Activity to upload generated dataset files to remote storage.

    Args:
        data: Configuration and input parameters for the dataset upload.

    Returns:
        str: Remote URI where dataset files are stored (e.g. s3://bucket/prefix/file.zip).
    """
    artifacts_dir = Path(action_instance_artifacts_dir(data.export_entries_workflow_id))

    export_order = (METADATA_FILE_NAME, MANIFEST_FILE_NAME, DATA_ARTIFACT_NAME)
    exportable_artifacts = discover_exportable_artifacts(artifacts_dir, export_order)

    storage_settings = data.storage_settings
    if (
        isinstance(storage_settings, S3StorageSettings)
        or getattr(storage_settings, 'storage_type', None) == 's3'
    ):
        return _upload_dataset_to_s3(
            data, storage_settings, exportable_artifacts, artifacts_dir
        )

    storage_type = getattr(
        storage_settings, 'storage_type', type(storage_settings).__name__
    )
    raise ValueError(f'Unsupported storage protocol: {storage_type}')


@activity.defn
async def copy_remote_dataset_to_upload(
    data: CopyRemoteDatasetToUploadInput,
) -> str:
    """Copy an S3-backed dataset into a NOMAD staging upload."""
    storage_settings = data.storage_settings
    if not isinstance(storage_settings, S3StorageSettings):
        raise ValueError(
            f'Unsupported storage protocol: {getattr(storage_settings, "storage_type", type(storage_settings))}'
        )
    return _copy_dataset_from_s3_to_upload(data, storage_settings)
