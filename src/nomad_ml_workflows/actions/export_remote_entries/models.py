from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_serializer

from nomad_ml_workflows.actions.export_entries.models import (
    ExportSettings,
    SearchSettings,
)


class S3StorageSettings(BaseModel):
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
        """Return model dict with secret credentials masked with **********."""
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
    model_config = ConfigDict(title='')

    user_id: str = Field(
        ..., description='Unique identifier for the user who initiated the workflow.'
    )
    search_settings: SearchSettings = Field(..., title='Search options')
    export_settings: ExportSettings = Field(..., title='Export options')
    storage_settings: RemoteStorageSettings = Field(..., title='Remote storage options')


class ExportRemoteDatasetInput(BaseModel):
    export_entries_workflow_id: str = Field(
        ..., description='ID of the export entries workflow.'
    )
    storage_settings: RemoteStorageSettings = Field(
        ..., description='Remote storage configuration.'
    )
    zip_output: bool = Field(
        ..., description='Whether to create a zip file for the exported dataset.'
    )
    exportable_dir_name: str = Field(
        ...,
        description='Name of the directory containing the dataset that will be exported.',
    )


class ExportRemoteEntriesOutput(BaseModel):
    remote_uri: str = Field(
        ...,
        description='URI of the exported dataset on remote storage (e.g. s3://bucket/prefix/file.zip).',
    )
    workflow_duration: float = Field(
        ...,
        description='Total duration of the Export Remote Entries workflow in seconds.',
    )
