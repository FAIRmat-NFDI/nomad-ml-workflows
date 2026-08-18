from nomad.actions import TaskQueue
from pydantic import Field
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nomad.config.models.plugins import ActionEntryPoint


class ExportEntriesActionEntryPoint(ActionEntryPoint):
    max_entries_export_limit: int = Field(
        default=100000,
        gt=0,
        description='Maximum number of entries that can be exported in a single '
        'Export Entries action.',
    )
    read_archives_timeout: int = Field(
        default=7200,  # 2 hours
        gt=0,
        description='Timeout (in seconds) for the activity that reads '
        'and writes the output file.',
    )
    write_tabular_timeout: int = Field(
        default=7200,  # 2 hours
        gt=0,
        description='Timeout (in seconds) for the activity that writes '
        'the output tabular file.',
    )
    max_write_buffer_size_bytes: int = Field(
        default=1024 * 1024 * 64,  # 64 MB
        gt=0,
        description='Maximum number of encoded NDJSON input bytes represented by '
        'parsed rows buffered before writing to the output tabular file. Protects '
        'against rows with large encoded representations. One oversized row may '
        'exceed this target.',
    )
    max_write_buffer_size_rows: int = Field(
        default=1024,
        gt=0,
        description='Maximum number of parsed rows buffered before writing to the '
        'output tabular file. Protects against many small or sparse rows whose '
        'Python and Arrow representations are much larger than their NDJSON bytes.',
    )

    def load(self):
        from nomad.actions import Action

        from nomad_ml_workflows.actions.export_entries.activities import (
            cleanup_artifacts,
            export_dataset_to_upload,
            prepare_manifest,
            read_archives_and_write_output_json,
            read_archives_and_write_table_rows,
            write_metadata_file,
            write_output_tabular,
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
                read_archives_and_write_table_rows,
                write_output_tabular,
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
