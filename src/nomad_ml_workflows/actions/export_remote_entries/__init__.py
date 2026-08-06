from nomad.actions import TaskQueue
from pydantic import Field
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nomad.config.models.plugins import ActionEntryPoint


class ExportRemoteEntriesActionEntryPoint(ActionEntryPoint):
    max_entries_export_limit: int = Field(
        default=100000,
        description='Maximum number of entries that can be exported in a single '
        'Export Remote Entries action.',
    )
    read_archives_timeout: int = Field(
        default=7200,  # 2 hours
        description='Timeout (in seconds) for the activity that reads '
        'and writes the output file.',
    )
    max_write_buffer_size_bytes: int = Field(
        default=1024 * 1024 * 4,  # 4 MB
        description='Maximum number of bytes to buffer before writing to the output '
        'tabular file. Increasing it can lead to higher memory usage but improved '
        'compression ratios.',
    )

    def load(self):
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
            upload_dataset_to_remote_storage,
        )
        from nomad_ml_workflows.actions.export_remote_entries.workflows import (
            ExportRemoteEntriesWorkflow,
        )

        return Action(
            task_queue=self.task_queue,
            workflow=ExportRemoteEntriesWorkflow,
            child_workflows=[ExtractEntriesWorkflow, ReadArchivesWorkflow],
            activities=[
                prepare_manifest,
                read_archives_and_write_output_json,
                read_archives_and_write_output_tabular,
                upload_dataset_to_remote_storage,
                cleanup_artifacts,
                write_metadata_file,
            ],
        )


export_remote_entries = ExportRemoteEntriesActionEntryPoint(  # type: ignore
    name='Export Remote Entries Action',
    description='An action to search entries and export them to remote storage.',
    task_queue=TaskQueue.CPU,
)
