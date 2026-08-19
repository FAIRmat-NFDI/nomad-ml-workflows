"""
Action entry point and configuration models for exporting remote entries.
"""

import os
from typing import TYPE_CHECKING, Literal

from nomad.actions import TaskQueue
from pydantic import BaseModel, Field, SecretStr
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nomad.config.models.plugins import ActionEntryPoint

if TYPE_CHECKING:
    from nomad.actions import Action

    from nomad_ml_workflows.actions.export_remote_entries.models import (
        S3StorageSettings,
    )

EXPORT_REMOTE_ENTRIES_ACTION_ENTRY_POINT_ID = (
    'nomad_ml_workflows.actions:export_remote_entries'
)


class NexusEndpointConfig(BaseModel):
    """Configuration for a remote Nexus endpoint."""

    display_name: str = Field(
        ...,
        description='Human-readable display name for the remote Nexus endpoint.',
    )
    endpoint: str = Field(
        ...,
        description='Temporal Nexus endpoint identifier registered in the Temporal cluster.',
    )


class ExportRemoteEntriesActionEntryPoint(ActionEntryPoint):
    """Action entry point for exporting entries across local and remote Oases."""

    # Defaulting users to ['admin'] makes this workflow opt-in; deployment admins
    # must explicitly override the allowed user IDs/groups in their nomad.yaml.
    users: list[str] | None = Field(
        default=['admin'],
        description='List of user IDs that are allowed to start/execute the given action.',
    )
    local_display_name: str = Field(
        default='Local Oasis',
        description='Display name for local execution on this deployment.',
    )
    nexus_endpoints: dict[str, NexusEndpointConfig] | None = Field(
        default=None,
        description='Mapping of remote Oasis keys to their Nexus endpoint configurations.',
    )
    s3_mode: Literal['env', 'workflow_input'] = Field(
        default='env',
        description=(
            'Controls whether S3 remote storage details are resolved from worker '
            'environment variables/config or supplied as workflow inputs in the form.'
        ),
    )
    s3_bucket: str | None = Field(
        default=None,
        description='Default S3 bucket name when s3_mode is `env`.',
    )
    s3_prefix: str | None = Field(
        default=None,
        description='Default S3 prefix when s3_mode is `env`.',
    )
    s3_endpoint_url: str | None = Field(
        default=None,
        description='Default custom S3 endpoint URL when s3_mode is `env`.',
    )
    s3_region: str | None = Field(
        default=None,
        description='Default S3 region when s3_mode is `env`.',
    )
    s3_access_key_id: SecretStr | None = Field(
        default=None,
        description='Default S3 access key ID when s3_mode is `env`.',
    )
    s3_secret_access_key: SecretStr | None = Field(
        default=None,
        description='Default S3 secret access key when s3_mode is `env`.',
    )
    s3_session_token: SecretStr | None = Field(
        default=None,
        description='Default S3 session token when s3_mode is `env`.',
    )

    def resolve_s3_storage_settings(self) -> 'S3StorageSettings':
        """Resolve S3 storage settings from entry point config with env fallback."""
        from nomad_ml_workflows.actions.export_remote_entries.models import (
            S3StorageSettings,
        )

        bucket = (
            self.s3_bucket
            or os.environ.get('S3_BUCKET')
            or os.environ.get('AWS_S3_BUCKET')
            or os.environ.get('AWS_BUCKET')
        )
        if not bucket:
            raise ValueError(
                "S3 bucket name is required when s3_mode is 'env'. "
                'Configure s3_bucket in entrypoint options or set S3_BUCKET / AWS_S3_BUCKET in the environment.'
            )

        prefix = (
            self.s3_prefix
            if self.s3_prefix is not None
            else (os.environ.get('S3_PREFIX') or os.environ.get('AWS_S3_PREFIX') or '')
        )

        endpoint_url = (
            self.s3_endpoint_url
            or os.environ.get('S3_ENDPOINT_URL')
            or os.environ.get('AWS_ENDPOINT_URL_S3')
            or os.environ.get('AWS_ENDPOINT_URL')
        )

        region = (
            self.s3_region
            or os.environ.get('S3_REGION')
            or os.environ.get('AWS_DEFAULT_REGION')
            or os.environ.get('AWS_REGION')
        )

        access_key_id = self.s3_access_key_id
        if access_key_id is None:
            env_key = os.environ.get('S3_ACCESS_KEY_ID') or os.environ.get(
                'AWS_ACCESS_KEY_ID'
            )
            if env_key:
                access_key_id = SecretStr(env_key)

        secret_access_key = self.s3_secret_access_key
        if secret_access_key is None:
            env_secret = os.environ.get('S3_SECRET_ACCESS_KEY') or os.environ.get(
                'AWS_SECRET_ACCESS_KEY'
            )
            if env_secret:
                secret_access_key = SecretStr(env_secret)

        session_token = self.s3_session_token
        if session_token is None:
            env_token = os.environ.get('S3_SESSION_TOKEN') or os.environ.get(
                'AWS_SESSION_TOKEN'
            )
            if env_token:
                session_token = SecretStr(env_token)

        return S3StorageSettings(
            storage_type='s3',
            bucket=bucket,
            prefix=prefix,
            endpoint_url=endpoint_url,
            region=region,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
        )

    def load(self) -> 'Action':
        """Load and assemble the Export Remote Entries Action instance."""
        from nomad.actions import Action

        from nomad_ml_workflows.actions.export_entries.activities import (
            cleanup_artifacts,
            prepare_manifest,
            read_archives_and_write_output_json,
            read_archives_and_write_table_rows,
            write_metadata_file,
            write_output_tabular,
        )
        from nomad_ml_workflows.actions.export_entries.workflows import (
            ExtractEntriesWorkflow,
            ReadArchivesWorkflow,
        )
        from nomad_ml_workflows.actions.export_remote_entries.activities import (
            copy_remote_dataset_to_upload,
            read_num_entries_exported,
            resolve_export_remote_entries_runtime_activity,
            upload_dataset_to_remote_storage,
        )
        from nomad_ml_workflows.actions.export_remote_entries.nexus_contract import (
            ExportRemoteEntriesServiceHandler,
        )
        from nomad_ml_workflows.actions.export_remote_entries.workflows import (
            ExportRemoteEntriesWorkflow,
        )

        return Action(
            task_queue=self.task_queue,
            workflow=ExportRemoteEntriesWorkflow,
            child_workflows=[
                ExtractEntriesWorkflow,
                ReadArchivesWorkflow,
            ],
            activities=[
                resolve_export_remote_entries_runtime_activity,
                prepare_manifest,
                read_archives_and_write_output_json,
                read_archives_and_write_table_rows,
                upload_dataset_to_remote_storage,
                copy_remote_dataset_to_upload,
                read_num_entries_exported,
                cleanup_artifacts,
                write_metadata_file,
                write_output_tabular,
            ],
            nexus_service_handlers=[ExportRemoteEntriesServiceHandler()],
        )


export_remote_entries_action_entry_point = (  # type: ignore
    ExportRemoteEntriesActionEntryPoint(
        name='Export Remote Entries Action',
        description=(
            'Search entries by running extraction on remote Oases over the Nexus network, '
            'then export the results to shared remote storage and, optionally, a local upload.'
        ),
        task_queue=TaskQueue.CPU,
        users=['admin'],
    )
)
export_remote_entries = export_remote_entries_action_entry_point


def current_export_remote_entries_action_entry_point() -> (
    ExportRemoteEntriesActionEntryPoint
):
    """Return the active ExportRemoteEntriesActionEntryPoint entry point, including config overrides."""
    try:
        from nomad.config import config as nomad_config

        if nomad_config.plugins is None:
            nomad_config.load_plugins()
        loaded_entry_point = nomad_config.get_plugin_entry_point(
            EXPORT_REMOTE_ENTRIES_ACTION_ENTRY_POINT_ID
        )
        if isinstance(loaded_entry_point, ExportRemoteEntriesActionEntryPoint):
            return loaded_entry_point
    except Exception:
        pass
    return export_remote_entries_action_entry_point
