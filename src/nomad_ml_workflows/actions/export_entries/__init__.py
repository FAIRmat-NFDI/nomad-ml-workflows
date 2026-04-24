from nomad.actions import TaskQueue
from pydantic import Field
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nomad.config.models.plugins import ActionEntryPoint


class ExportEntriesActionEntryPoint(ActionEntryPoint):
    search_workflow_concurrency_limit: int = Field(
        default=5,
        description='Number of child search workflow instances to run concurrently in '
        'the Export Entries action. Keep this low to avoid overwhelming the Temporal '
        'server with too many concurrent activities.',
    )
    search_batch_timeout: int = Field(
        default=7200,  # 2 hours
        description='Timeout (in seconds) for each search batch in the Export Entries '
        'action. Set this accordingly to time out longer searches.',
    )
    max_entries_export_limit: int = Field(
        default=100000,
        description='Maximum number of entries that can be exported in a single '
        'Export Entries action.',
    )

    def load(self):
        from nomad.actions import Action

        from nomad_ml_workflows.actions.export_entries.activities import (
            cleanup_artifacts,
            collect_page_cursors,
            create_artifact_subdirectory,
            export_dataset_to_upload,
            merge_output_files,
            search,
        )
        from nomad_ml_workflows.actions.export_entries.workflows import (
            ExportEntriesWorkflow,
            SearchPageWorkflow,
        )

        return Action(
            task_queue=self.task_queue,
            workflow=ExportEntriesWorkflow,
            child_workflows=[SearchPageWorkflow],
            activities=[
                create_artifact_subdirectory,
                collect_page_cursors,
                search,
                merge_output_files,
                export_dataset_to_upload,
                cleanup_artifacts,
            ],
        )


export_entries = ExportEntriesActionEntryPoint(
    name='Export Entries Action',
    description='An action to search entries and export them as a zip file in the '
    'specified upload.',
    task_queue=TaskQueue.CPU,
)
