"""
Nexus RPC contract and service handler for cross-Oasis remote entry export.
"""

import uuid
from typing import TYPE_CHECKING

import nexusrpc
from pydantic import BaseModel, Field
from temporalio import nexus

from nomad_ml_workflows.actions.export_entries.models import (
    ExportSettings,
    SearchSettings,
)
from nomad_ml_workflows.actions.export_remote_entries.models import (
    RemoteStorageSettings,
)

if TYPE_CHECKING:
    pass


class RemoteExtractInput(BaseModel):
    """Input payload for remote extraction via Nexus RPC."""

    user_id: str = Field(..., description='Originating user ID.')
    search_settings: SearchSettings = Field(
        ..., description='Search query and options.'
    )
    export_settings: ExportSettings = Field(..., description='Export settings.')
    storage_settings: RemoteStorageSettings | None = Field(
        None, description='Optional remote storage settings.'
    )


class RemoteExtractOutput(BaseModel):
    """Output result returned by remote extraction via Nexus RPC."""

    target_oasis_key: str = Field('', description='Key identifying the target Oasis.')
    status: str = Field(..., description='Status of extraction (SUCCESS or FAILED).')
    num_entries_exported: int = Field(0, description='Number of entries exported.')
    remote_uri: str | None = Field(
        None, description='Remote URI or S3 URI if uploaded.'
    )
    error_message: str | None = Field(None, description='Error message if failed.')


@nexusrpc.service
class ExportRemoteEntriesService:
    """Nexus RPC service defining remote entry export operations."""

    export_remote_entries: nexusrpc.Operation[RemoteExtractInput, RemoteExtractOutput]


@nexusrpc.handler.service_handler(service=ExportRemoteEntriesService)
class ExportRemoteEntriesServiceHandler:
    """Service handler executing Export Remote Entries operations."""

    @nexus.workflow_run_operation
    def export_remote_entries(
        self,
        ctx: nexus.WorkflowRunOperationContext,
        extract_input: RemoteExtractInput,
    ) -> nexus.WorkflowHandle[RemoteExtractOutput]:
        """Initiate the ExportRemoteEntriesWorkflow for a Nexus RPC request."""
        from nomad_ml_workflows.actions.export_remote_entries.models import (
            ExportRemoteEntriesUserInput,
            S3StorageSettings,
        )
        from nomad_ml_workflows.actions.export_remote_entries.workflows import (
            ExportRemoteEntriesWorkflow,
        )

        storage_settings = extract_input.storage_settings or S3StorageSettings(
            bucket='default'
        )
        user_input = ExportRemoteEntriesUserInput(
            user_id=extract_input.user_id,
            target_oases=['local'],
            search_settings=extract_input.search_settings,
            export_settings=extract_input.export_settings,
            storage_settings=storage_settings,
        )
        return ctx.start_workflow(
            ExportRemoteEntriesWorkflow.run,
            user_input,
            id=f'export-remote-entries-{extract_input.user_id}-{uuid.uuid4()}',
        )
