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
    assert target_oases_props['items']['enum'] == ['local', 'oasis_b', 'oasis_c']
    assert target_oases_props['uniqueItems'] is True
    assert target_oases_props['uiSchema']['ui:enumNames'] == [
        'Local (Oasis A)',
        'Oasis B (DESY)',
        'Oasis C (HZB)',
    ]


def test_export_remote_entries_schema_omits_storage_settings_in_env_mode(monkeypatch):
    from nomad_ml_workflows.actions.export_remote_entries import (
        ExportRemoteEntriesActionEntryPoint,
    )

    monkeypatch.setattr(
        'nomad.config.models.config.Config.get_plugin_entry_point',
        lambda self, key: ExportRemoteEntriesActionEntryPoint(s3_mode='env'),
    )
    schema = ExportRemoteEntriesUserInput.model_json_schema()
    assert 'storage_settings' not in schema.get('properties', {})
    assert 'storage_settings' not in schema.get('required', [])


def test_export_remote_entries_schema_exposes_storage_settings_in_workflow_input_mode(
    monkeypatch,
):
    from nomad_ml_workflows.actions.export_remote_entries import (
        ExportRemoteEntriesActionEntryPoint,
    )

    monkeypatch.setattr(
        'nomad.config.models.config.Config.get_plugin_entry_point',
        lambda self, key: ExportRemoteEntriesActionEntryPoint(s3_mode='workflow_input'),
    )
    schema = ExportRemoteEntriesUserInput.model_json_schema()
    assert 'storage_settings' in schema['properties']
    assert 'storage_settings' in schema['required']


def test_resolve_s3_storage_settings_from_entrypoint_and_env(monkeypatch):
    from nomad_ml_workflows.actions.export_remote_entries import (
        ExportRemoteEntriesActionEntryPoint,
    )

    # 1. Test resolution from entrypoint config
    ep = ExportRemoteEntriesActionEntryPoint(
        s3_bucket='entrypoint-bucket',
        s3_prefix='custom/prefix',
        s3_endpoint_url='https://minio.custom.org',
        s3_region='eu-west-1',
        s3_access_key_id=SecretStr('ep-key'),
        s3_secret_access_key=SecretStr('ep-secret'),
        s3_session_token=SecretStr('ep-token'),
    )
    settings = ep.resolve_s3_storage_settings()
    assert settings.bucket == 'entrypoint-bucket'
    assert settings.prefix == 'custom/prefix'
    assert settings.endpoint_url == 'https://minio.custom.org'
    assert settings.region == 'eu-west-1'
    assert settings.access_key_id.get_secret_value() == 'ep-key'
    assert settings.secret_access_key.get_secret_value() == 'ep-secret'
    assert settings.session_token.get_secret_value() == 'ep-token'

    # 2. Test fallback to environment variables
    ep_empty = ExportRemoteEntriesActionEntryPoint()
    monkeypatch.setenv('S3_BUCKET', 'env-bucket')
    monkeypatch.setenv('S3_PREFIX', 'env/prefix')
    monkeypatch.setenv('AWS_ENDPOINT_URL_S3', 'https://s3.env.org')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-2')
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'env-key')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'env-secret')
    monkeypatch.setenv('AWS_SESSION_TOKEN', 'env-token')

    settings_env = ep_empty.resolve_s3_storage_settings()
    assert settings_env.bucket == 'env-bucket'
    assert settings_env.prefix == 'env/prefix'
    assert settings_env.endpoint_url == 'https://s3.env.org'
    assert settings_env.region == 'us-east-2'
    assert settings_env.access_key_id.get_secret_value() == 'env-key'
    assert settings_env.secret_access_key.get_secret_value() == 'env-secret'
    assert settings_env.session_token.get_secret_value() == 'env-token'

    # 3. Test explicit empty prefix overrides env variable
    ep_root = ExportRemoteEntriesActionEntryPoint(s3_prefix='')
    settings_root = ep_root.resolve_s3_storage_settings()
    assert settings_root.prefix == ''

    # 4. Test error when bucket is missing
    monkeypatch.delenv('S3_BUCKET')
    with pytest.raises(ValueError, match='S3 bucket name is required'):
        ep_empty.resolve_s3_storage_settings()


def test_normalize_s3_storage_input():
    from temporalio.exceptions import ApplicationError

    from nomad_ml_workflows.actions.export_remote_entries.models import (
        ResolveExportRemoteEntriesRuntimeOutput,
    )
    from nomad_ml_workflows.actions.export_remote_entries.workflows import (
        ExportRemoteEntriesWorkflow,
    )

    user_input = ExportRemoteEntriesUserInput(
        user_id='user1',
        target_oases=['local'],
        search_settings={
            'owner': 'visible',
            'max_entries': 10,
            'query': '{}',
            'required': [],
        },
        export_settings={'file_format': 'parquet', 'create_zip_archive': True},
    )

    # In workflow_input mode without storage_settings -> error
    runtime_workflow_input = ResolveExportRemoteEntriesRuntimeOutput(
        s3_mode='workflow_input'
    )
    with pytest.raises(ApplicationError, match='S3 storage settings are required'):
        ExportRemoteEntriesWorkflow._normalize_s3_storage_input(
            user_input, runtime_workflow_input
        )

    # In env mode with resolved settings -> successfully populates storage_settings
    resolved = S3StorageSettings(bucket='auto-bucket')
    runtime_env = ResolveExportRemoteEntriesRuntimeOutput(
        s3_mode='env',
        resolved_storage_settings=resolved,
    )
    normalized = ExportRemoteEntriesWorkflow._normalize_s3_storage_input(
        user_input, runtime_env
    )
    assert normalized.storage_settings is not None
    assert normalized.storage_settings.bucket == 'auto-bucket'
