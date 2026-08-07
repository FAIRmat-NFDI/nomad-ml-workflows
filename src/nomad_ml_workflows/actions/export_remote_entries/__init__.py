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

    local_display_name: str = Field(
        default='Local Oasis',
        description='Display name for local execution on this deployment.',
    )
    nexus_endpoints: dict[str, NexusEndpointConfig] | None = Field(
        default=None,
        description='Mapping of remote Oasis keys to their Nexus endpoint configurations.',
    )
    max_entries_export_limit: int = Field(
        default=100000,
        description='Maximum number of entries that can be exported in a single action.',
    )
    read_archives_timeout: int = Field(
        default=7200,  # 2 hours
        description='Timeout (in seconds) for reading archives and writing output.',
    )
    max_write_buffer_size_bytes: int = Field(
        default=1024 * 1024 * 4,  # 4 MB
        description='Maximum bytes to buffer before writing output tabular file.',
    )

    def load(self) -> 'Action':
        """Load and assemble the Export Remote Entries Action instance."""
        from nomad.actions import Action

        from nomad_ml_workflows.actions.export_entries.activities import (
            cleanup_artifacts,
            prepare_manifest,
            read_archives_and_write_output_json,
            read_archives_and_write_output_tabular,
            write_metadata_file,
        )
        from nomad_ml_workflows.actions.export_entries.workflows import (
            ExtractEntriesWorkflow,
            ReadArchivesWorkflow,
        )
        from nomad_ml_workflows.actions.export_remote_entries.activities import (
            copy_remote_dataset_to_upload,
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
                read_archives_and_write_output_tabular,
                upload_dataset_to_remote_storage,
                copy_remote_dataset_to_upload,
                cleanup_artifacts,
                write_metadata_file,
            ],
            nexus_service_handlers=[ExportRemoteEntriesServiceHandler()],
        )


export_remote_entries = ExportRemoteEntriesActionEntryPoint(  # type: ignore
    name='Export Remote Entries Action',
    description='An action to search entries and export them to remote storage.',
    task_queue=TaskQueue.CPU,
)
