import json

import pytest
from pydantic import SecretStr

from nomad_ml_workflows.actions.export_remote_entries.models import (
    ExportRemoteDatasetInput,
    ExportRemoteEntriesOutput,
    ExportRemoteEntriesUserInput,
    S3StorageSettings,
)


def test_s3_storage_settings_defaults_and_secrets():
    settings = S3StorageSettings(
        bucket='test-bucket',
        prefix='exports/dataset',
        access_key_id=SecretStr('my_access_key'),
        secret_access_key=SecretStr('my_secret_key'),
    )

    assert settings.storage_type == 's3'
    assert settings.bucket == 'test-bucket'
    assert settings.prefix == 'exports/dataset'
    assert settings.access_key_id.get_secret_value() == 'my_access_key'
    assert settings.secret_access_key.get_secret_value() == 'my_secret_key'
    assert settings.session_token is None

    # Verify secret redaction
    redacted = settings.dump_redacted()
    assert redacted['access_key_id'] == '**********'
    assert redacted['secret_access_key'] == '**********'
    assert redacted['bucket'] == 'test-bucket'

    # Verify model_dump_json serializes secrets for Temporal payloads
    json_data = json.loads(settings.model_dump_json())
    assert json_data['access_key_id'] == 'my_access_key'
    assert json_data['secret_access_key'] == 'my_secret_key'


def test_export_remote_entries_user_input_discriminated_union():
    user_input_data = {
        'user_id': 'user_123',
        'target_oases': ['local'],
        'search_settings': {
            'owner': 'visible',
            'max_entries': 100,
            'query': '{"entry_type": "ELNSample"}',
            'required': [],
        },
        'export_settings': {
            'file_format': 'parquet',
            'create_zip_archive': True,
        },
        'storage_settings': {
            'storage_type': 's3',
            'bucket': 'my-remote-bucket',
            'prefix': 'nomad-data',
            'endpoint_url': 'https://minio.example.com',
            'region': 'us-east-1',
            'access_key_id': 'key_123',
            'secret_access_key': 'secret_456',
        },
    }

    user_input = ExportRemoteEntriesUserInput(**user_input_data)
    assert user_input.user_id == 'user_123'
    assert isinstance(user_input.storage_settings, S3StorageSettings)
    assert user_input.storage_settings.storage_type == 's3'
    assert user_input.storage_settings.bucket == 'my-remote-bucket'
    assert user_input.storage_settings.endpoint_url == 'https://minio.example.com'
    assert user_input.storage_settings.access_key_id.get_secret_value() == 'key_123'


def test_save_to_upload_requires_upload_id():
    with pytest.raises(ValueError, match='upload_id is required'):
        ExportRemoteEntriesUserInput(
            user_id='user_123',
            save_to_upload=True,
            target_oases=['local'],
            search_settings={
                'owner': 'visible',
                'max_entries': 100,
                'query': '{}',
                'required': [],
            },
            export_settings={
                'file_format': 'parquet',
                'create_zip_archive': True,
            },
            storage_settings={
                'storage_type': 's3',
                'bucket': 'test-bucket',
            },
        )


def test_export_remote_dataset_input_and_output():
    dataset_input = ExportRemoteDatasetInput(
        export_entries_workflow_id='workflow-123',
        storage_settings=S3StorageSettings(
            bucket='b1',
            prefix='p1',
        ),
        zip_output=True,
        exportable_dir_name='export_entries_2026-08-06',
    )
    assert dataset_input.export_entries_workflow_id == 'workflow-123'
    assert dataset_input.zip_output is True

    expected_workflow_duration = 12.34
    output = ExportRemoteEntriesOutput(
        remote_uri='s3://b1/p1/export_entries_2026-08-06.zip',
        workflow_duration=expected_workflow_duration,
    )
    assert output.remote_uri == 's3://b1/p1/export_entries_2026-08-06.zip'
    assert output.workflow_duration == expected_workflow_duration


def test_get_schema_for_entry_point():
    class DummyEndpoint:
        def __init__(self, display_name):
            self.display_name = display_name

    class DummyEntryPointConfig:
        local_display_name = 'Local (Oasis A)'
        nexus_endpoints = {
            'oasis_b': DummyEndpoint('Oasis B (DESY)'),
            'oasis_c': DummyEndpoint('Oasis C (HZB)'),
        }

    DynamicModel = ExportRemoteEntriesUserInput.get_schema_for_entry_point(
        DummyEntryPointConfig()
    )
    schema = DynamicModel.model_json_schema()
    target_oases_props = schema['properties']['target_oases']

    assert target_oases_props['enum'] == ['local', 'oasis_b', 'oasis_c']
    assert target_oases_props['uiSchema']['ui:enumNames'] == [
        'Local (Oasis A)',
        'Oasis B (DESY)',
        'Oasis C (HZB)',
    ]
