from unittest.mock import MagicMock, patch

import pytest
from nomad.files import StagingUploadFiles
from pydantic import SecretStr

from nomad_ml_workflows.actions.export_remote_entries.activities import (
    copy_remote_dataset_to_upload,
    read_num_entries_exported,
    upload_dataset_to_remote_storage,
)
from nomad_ml_workflows.actions.export_remote_entries.models import (
    CopyRemoteDatasetToUploadInput,
    ExportRemoteDatasetInput,
    S3StorageSettings,
)


@patch(
    'nomad_ml_workflows.actions.export_remote_entries.activities.action_instance_artifacts_dir'
)
def test_read_num_entries_exported(mock_artifacts_dir, tmp_path):
    expected_num_entries = 42
    artifacts_dir = tmp_path / 'artifacts'
    artifacts_dir.mkdir()
    (artifacts_dir / 'metadata.json').write_text(
        f'{{"data": {{"num_entries_exported": {expected_num_entries}}}}}'
    )
    mock_artifacts_dir.return_value = artifacts_dir.as_posix()

    assert read_num_entries_exported('wf-001') == expected_num_entries


@pytest.mark.asyncio
@patch('nomad_ml_workflows.actions.export_remote_entries.activities.boto3.client')
@patch(
    'nomad_ml_workflows.actions.export_remote_entries.activities.action_instance_artifacts_dir'
)
async def test_upload_dataset_to_remote_storage_s3_zipped(
    mock_artifacts_dir, mock_boto_client, tmp_path
):
    # Setup mock artifacts directory with sample files
    artifacts_dir = tmp_path / 'artifacts'
    artifacts_dir.mkdir()
    (artifacts_dir / 'metadata.json').write_text('{}')
    (artifacts_dir / 'selected_entries.json').write_text('[]')
    (artifacts_dir / 'data.parquet').write_text('binary data')
    mock_artifacts_dir.return_value = artifacts_dir.as_posix()

    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    storage_settings = S3StorageSettings(
        bucket='my-bucket',
        prefix='my-folder/',
        endpoint_url='https://minio.local:9000',
        region='eu-central-1',
        access_key_id=SecretStr('access123'),
        secret_access_key=SecretStr('secret456'),
        session_token=SecretStr('token789'),
    )

    dataset_input = ExportRemoteDatasetInput(
        export_entries_workflow_id='wf-001',
        storage_settings=storage_settings,
        zip_output=True,
        exportable_dir_name='export_entries_2026',
    )

    remote_uri = await upload_dataset_to_remote_storage(dataset_input)

    # Check boto3 client creation kwargs
    mock_boto_client.assert_called_once_with(
        's3',
        endpoint_url='https://minio.local:9000',
        region_name='eu-central-1',
        aws_access_key_id='access123',
        aws_secret_access_key='secret456',
        aws_session_token='token789',
    )

    # Check S3 upload_file call
    mock_s3.upload_file.assert_called_once()
    call_args = mock_s3.upload_file.call_args[0]
    assert call_args[1] == 'my-bucket'
    assert call_args[2] == 'my-folder/export_entries_2026.zip'
    assert remote_uri == 's3://my-bucket/my-folder/export_entries_2026.zip'


@pytest.mark.asyncio
@patch('nomad_ml_workflows.actions.export_remote_entries.activities.boto3.client')
@patch(
    'nomad_ml_workflows.actions.export_remote_entries.activities.action_instance_artifacts_dir'
)
async def test_upload_dataset_to_remote_storage_s3_unzipped(
    mock_artifacts_dir, mock_boto_client, tmp_path
):
    artifacts_dir = tmp_path / 'artifacts'
    artifacts_dir.mkdir()
    (artifacts_dir / 'metadata.json').write_text('{}')
    (artifacts_dir / 'selected_entries.json').write_text('[]')
    (artifacts_dir / 'data.csv').write_text('a,b\n1,2')
    mock_artifacts_dir.return_value = artifacts_dir.as_posix()

    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    storage_settings = S3StorageSettings(
        bucket='my-bucket',
        prefix='',
    )

    dataset_input = ExportRemoteDatasetInput(
        export_entries_workflow_id='wf-002',
        storage_settings=storage_settings,
        zip_output=False,
        exportable_dir_name='export_entries_2026',
    )

    remote_uri = await upload_dataset_to_remote_storage(dataset_input)

    expected_upload_count = 3
    assert mock_s3.upload_file.call_count == expected_upload_count
    uploaded_keys = [call[0][2] for call in mock_s3.upload_file.call_args_list]
    assert 'export_entries_2026/metadata.json' in uploaded_keys
    assert 'export_entries_2026/selected_entries.json' in uploaded_keys
    assert 'export_entries_2026/data.csv' in uploaded_keys
    assert remote_uri == 's3://my-bucket/export_entries_2026/'


@pytest.mark.asyncio
@patch('nomad_ml_workflows.actions.export_remote_entries.activities.boto3.client')
@patch(
    'nomad_ml_workflows.actions.export_remote_entries.activities.get_upload_files'
)
async def test_copy_remote_dataset_to_upload_s3_zipped(
    mock_get_upload_files, mock_boto_client
):
    mock_upload_files = MagicMock(spec=StagingUploadFiles)
    mock_upload_files.raw_path_exists.return_value = False
    mock_get_upload_files.return_value = mock_upload_files

    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    data = CopyRemoteDatasetToUploadInput(
        user_id='user-123',
        upload_id='upload-123',
        remote_uri='s3://my-bucket/exports/dataset.zip',
        storage_settings=S3StorageSettings(
            bucket='my-bucket',
            endpoint_url='https://s3.example.com',
        ),
        zip_output=True,
    )

    result = await copy_remote_dataset_to_upload(data)

    assert result == 'dataset.zip'
    mock_s3.download_file.assert_called_once()
    download_args = mock_s3.download_file.call_args[0]
    assert download_args[:2] == ('my-bucket', 'exports/dataset.zip')
    mock_upload_files.add_rawfiles.assert_called_once_with(
        target_path=download_args[2], auto_decompress=False
    )


@pytest.mark.asyncio
@patch('nomad_ml_workflows.actions.export_remote_entries.activities.boto3.client')
@patch(
    'nomad_ml_workflows.actions.export_remote_entries.activities.get_upload_files'
)
async def test_copy_remote_dataset_to_upload_s3_directory(
    mock_get_upload_files, mock_boto_client
):
    mock_upload_files = MagicMock(spec=StagingUploadFiles)
    mock_upload_files.raw_path_exists.return_value = False
    mock_get_upload_files.return_value = mock_upload_files

    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {
        'Contents': [
            {'Key': 'exports/dataset/metadata.json'},
            {'Key': 'exports/dataset/data.csv'},
        ]
    }
    mock_boto_client.return_value = mock_s3

    data = CopyRemoteDatasetToUploadInput(
        user_id='user-123',
        upload_id='upload-123',
        remote_uri='s3://my-bucket/exports/dataset/',
        storage_settings=S3StorageSettings(bucket='my-bucket'),
        zip_output=False,
    )

    result = await copy_remote_dataset_to_upload(data)

    assert result == 'dataset'
    expected_download_count = 2
    assert mock_s3.download_file.call_count == expected_download_count
    mock_upload_files.add_rawfiles.assert_called_once()
    assert mock_upload_files.add_rawfiles.call_args.kwargs['target_dir'] == 'dataset'
