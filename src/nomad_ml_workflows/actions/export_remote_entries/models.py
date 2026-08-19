"""
Data models and schemas for Export Remote Entries workflows and activities.
"""

from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_serializer,
    model_validator,
)

from nomad_ml_workflows.actions.export_entries.models import (
    ExportSettings,
    SearchSettings,
)


class S3StorageSettings(BaseModel):
    """Configuration settings for S3-compatible remote object storage."""

    storage_type: Literal['s3'] = Field('s3', title='Storage Protocol')
    bucket: str = Field(..., title='Bucket Name', description='S3 bucket name.')
    prefix: str = Field(
        '',
        title='Prefix / Path Prefix',
        description='Folder or path prefix within the S3 bucket.',
    )
    endpoint_url: str | None = Field(
        None,
        title='Endpoint URL',
        description='Custom S3 endpoint URL (e.g. for MinIO, Wasabi, Cloudflare R2, Ceph).',
    )
    region: str | None = Field(
        None,
        title='Region',
        description='S3 region name.',
    )
    access_key_id: SecretStr | None = Field(
        None,
        title='Access Key ID',
        description='S3 access key ID.',
    )
    secret_access_key: SecretStr | None = Field(
        None,
        title='Secret Access Key',
        description='S3 secret access key.',
    )
    session_token: SecretStr | None = Field(
        None,
        title='Session Token',
        description='Optional S3 session token for temporary credentials.',
    )

    @field_serializer(
        'access_key_id', 'secret_access_key', 'session_token', mode='plain'
    )
    def _serialize_secret(self, v: SecretStr | None) -> str | None:
        return v.get_secret_value() if v else None

    def dump_redacted(self) -> dict[str, Any]:
        """Return model dictionary with secret credentials masked as '**********'."""
        data = self.model_dump()
        for key in ('access_key_id', 'secret_access_key', 'session_token'):
            if data.get(key) is not None:
                data[key] = '**********'
        return data


RemoteStorageSettings = Annotated[
    S3StorageSettings,
    Field(discriminator='storage_type'),
]


class ExportRemoteEntriesUserInput(BaseModel):
    """User-provided parameters for the Export Remote Entries workflow."""

    model_config = ConfigDict(title='')

    user_id: str = Field(
        ..., description='Unique identifier for the user who initiated the workflow.'
    )
    save_to_upload: bool = Field(
        False,
        title='Save dataset to upload',
        description='Copy the exported dataset from S3 into a staging upload.',
    )
    upload_id: str | None = Field(
        None,
        title='Destination project ID',
        description='Staging project/upload that receives the copied dataset.',
    )
    target_oases: list[str] = Field(
        title='Target Oases',
        description='Select target Oases for entry extraction.',
        # json_schema_extra={
        #     'enum': ['local'],
        #     'uiSchema': {
        #         'ui:widget': 'checkboxes',
        #         'ui:enumNames': ['Local Oasis'],
        #     },
        # },
    )
    search_settings: SearchSettings = Field(..., title='Search options')
    export_settings: ExportSettings = Field(
        default_factory=ExportSettings, title='Export options'
    )
    storage_settings: RemoteStorageSettings | None = Field(
        default=None,
        title='Remote storage options',
        description='S3 storage options used when s3_mode is workflow_input.',
    )

    @model_validator(mode='after')
    def validate_upload_destination(self) -> 'ExportRemoteEntriesUserInput':
        """Require an upload destination when saving the dataset locally."""
        if self.save_to_upload and not self.upload_id:
            raise ValueError('upload_id is required when save_to_upload is enabled.')
        return self

    @classmethod
    def model_json_schema(cls, *args, **kwargs):
        """Sets the required property of `storage_settings` based on `s3_mode`."""
        schema = super().model_json_schema(*args, **kwargs)
        try:
            from nomad_ml_workflows.actions.export_remote_entries import (
                current_export_remote_entries_action_entry_point,
            )

            s3_mode = current_export_remote_entries_action_entry_point().s3_mode
        except Exception:
            s3_mode = 'env'

        if s3_mode == 'workflow_input':
            required = schema.setdefault('required', [])
            if 'storage_settings' not in required:
                required.append('storage_settings')
            return schema

        schema.get('properties', {}).pop('storage_settings', None)
        required = schema.get('required', [])
        if 'storage_settings' in required:
            required.remove('storage_settings')
        if not required:
            schema.pop('required', None)
        return schema

    @classmethod
    def get_schema_for_entry_point(
        cls, entry_point_config: Any
    ) -> type['ExportRemoteEntriesUserInput']:
        """Dynamically construct a subclass of ExportRemoteEntriesUserInput with
        updated Pydantic field metadata (enum and uiSchema) reflecting configured
        remote Nexus endpoints.
        """
        options = ['local']
        labels = [getattr(entry_point_config, 'local_display_name', 'Local Oasis')]

        nexus_endpoints = getattr(entry_point_config, 'nexus_endpoints', None)
        if isinstance(nexus_endpoints, dict):
            for key, ep in nexus_endpoints.items():
                options.append(key)
                labels.append(getattr(ep, 'display_name', key))

        class DynamicExportRemoteEntriesUserInput(cls):  # type: ignore
            target_oases: list[str] = Field(
                default=['local'],
                title='Target Oases',
                description='Select target Oases for entry extraction.',
                json_schema_extra={
                    'enum': options,
                    'items': {'type': 'string', 'enum': options},
                    'uniqueItems': True,
                    'uiSchema': {
                        'ui:widget': 'checkboxes',
                        'ui:enumNames': labels,
                    },
                },
            )

        return DynamicExportRemoteEntriesUserInput


class ResolveExportRemoteEntriesRuntimeOutput(BaseModel):
    """Runtime configuration resolved for Export Remote Entries execution."""

    s3_mode: Literal['env', 'workflow_input'] = 'env'
    resolved_storage_settings: S3StorageSettings | None = None


class ExportRemoteDatasetInput(BaseModel):
    """Input parameters for uploading exported dataset files to remote storage."""

    export_entries_workflow_id: str = Field(
        ..., description='ID of the export entries workflow.'
    )
    storage_settings: RemoteStorageSettings = Field(
        ..., description='Remote storage configuration.'
    )
    zip_output: bool = Field(
        True, description='Whether to create a zip file for the exported dataset.'
    )
    exportable_dir_name: str = Field(
        ...,
        description='Name of the directory containing the dataset that will be exported.',
    )


class CopyRemoteDatasetToUploadInput(BaseModel):
    """Input parameters for copying an S3 dataset into a staging upload."""

    user_id: str = Field(..., description='User ID performing the copy.')
    upload_id: str = Field(..., description='Destination staging upload ID.')
    remote_uri: str = Field(..., description='S3 URI of the exported dataset.')
    storage_settings: RemoteStorageSettings = Field(
        ..., description='S3 storage configuration.'
    )
    zip_output: bool = Field(
        True, description='Whether the remote dataset is a ZIP archive.'
    )


class OasisExecutionResult(BaseModel):
    """Execution status and output summary for a target Oasis."""

    target_key: str = Field(..., description='Key identifying the target Oasis.')
    status: str = Field(..., description='Execution status (SUCCESS or FAILED).')
    is_remote: bool = Field(..., description='Whether execution was remote via Nexus.')
    num_entries_exported: int = Field(0, description='Number of entries exported.')
    remote_uri: str | None = Field(None, description='Remote URI for uploaded dataset.')
    error_message: str | None = Field(
        None, description='Error message if execution failed.'
    )


class ExportRemoteEntriesOutput(BaseModel):
    """Final summary output returned by the Export Remote Entries workflow."""

    results: dict[str, OasisExecutionResult] = Field(
        default_factory=dict,
        description='Execution results per target Oasis.',
    )
    total_entries_exported: int = Field(
        0, description='Total entries exported across all target Oases.'
    )
    remote_uri: str = Field(
        '',
        description='Primary URI of the exported dataset on remote storage (e.g. s3://bucket/prefix/file.zip).',
    )
    workflow_duration: float = Field(
        ...,
        description='Total duration of the Export Remote Entries workflow in seconds.',
    )
