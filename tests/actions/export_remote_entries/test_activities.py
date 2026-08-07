from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from nomad_ml_workflows.actions.export_remote_entries.activities import (
    upload_dataset_to_remote_storage,
)
from nomad_ml_workflows.actions.export_remote_entries.models import (
    ExportRemoteDatasetInput,
    S3StorageSettings,
)


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
