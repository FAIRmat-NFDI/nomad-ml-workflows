"""
Action entry point and configuration models for exporting remote entries.
"""

from typing import TYPE_CHECKING

from nomad.actions import TaskQueue
from pydantic import BaseModel, Field
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nomad.config.models.plugins import ActionEntryPoint

if TYPE_CHECKING:
    from nomad.actions import Action


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


export_remote_entries = ExportRemoteEntriesActionEntryPoint(  # type: ignore
    name='Export Remote Entries Action',
    description=(
        'Search entries by running extraction on remote Oases over the Nexus network, '
        'then export the results to shared remote storage and, optionally, a local upload.'
    ),
    task_queue=TaskQueue.CPU,
    users=['admin'],
)
