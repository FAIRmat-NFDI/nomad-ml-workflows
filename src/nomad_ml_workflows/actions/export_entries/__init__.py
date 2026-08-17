from nomad.actions import TaskQueue
from pydantic import Field
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nomad.config.models.plugins import ActionEntryPoint


class ExportEntriesActionEntryPoint(ActionEntryPoint):
    max_entries_export_limit: int = Field(
        default=100000,
        description='Maximum number of entries that can be exported in a single '
        'Export Entries action.',
    )
    read_archives_timeout: int = Field(
        default=7200,  # 2 hours
        description='Timeout (in seconds) for the activity that reads '
        'and writes the output file.',
    )
    max_write_buffer_size_bytes: int = Field(
        default=1024 * 1024 * 4,  # 4 MB
        description='Maximum number of encoded NDJSON input bytes represented by '
        'parsed rows buffered before writing to the output tabular file.',
    )

    def load(self):
        from nomad.actions import Action

        from nomad_ml_workflows.actions.export_entries.activities import (
            cleanup_artifacts,
            export_dataset_to_upload,
            prepare_manifest,
            read_archives_and_write_output_json,
            read_archives_and_write_output_tabular,
            write_metadata_file,
        )
        from nomad_ml_workflows.actions.export_entries.workflows import (
            ExportEntriesWorkflow,
            ExtractEntriesWorkflow,
            ReadArchivesWorkflow,
        )

        return Action(
            task_queue=self.task_queue,
            workflow=ExportEntriesWorkflow,
            child_workflows=[ExtractEntriesWorkflow, ReadArchivesWorkflow],
            activities=[
                prepare_manifest,
                read_archives_and_write_output_json,
                read_archives_and_write_output_tabular,
                export_dataset_to_upload,
                cleanup_artifacts,
                write_metadata_file,
            ],
        )


export_entries = ExportEntriesActionEntryPoint(  # type: ignore
    name='Export Entries Action',
    description='An action to search entries and export them in the specified upload.',
    task_queue=TaskQueue.CPU,
)
