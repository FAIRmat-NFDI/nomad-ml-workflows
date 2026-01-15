from nomad.actions import TaskQueue
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nomad.config.models.plugins import ActionEntryPoint


class ExportEntriesActionEntryPoint(ActionEntryPoint):
    def load(self):
        from nomad.actions import Action

        from nomad_actions.actions.entries.activities import (
            cleanup_artifacts,
            create_artifact_subdirectory,
            export_dataset_to_upload,
            merge_output_files,
            search,
        )
        from nomad_actions.actions.entries.workflows import ExportEntriesWorkflow

        return Action(
            task_queue=self.task_queue,
            workflow=ExportEntriesWorkflow,
            activities=[
                create_artifact_subdirectory,
                search,
                merge_output_files,
                export_dataset_to_upload,
                cleanup_artifacts,
            ],
        )


export_entries_action_entry_point = ExportEntriesActionEntryPoint(
    name='Export Entries Action',
    description='An action to search entries and export them into a datafile in the '
    'specified upload.',
    task_queue=TaskQueue.CPU,
)
