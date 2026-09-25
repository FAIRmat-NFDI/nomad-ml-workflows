from nomad.actions import TaskQueue
from pydantic import Field
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from nomad.config.models.plugins import ActionEntryPoint


class ExportForcesActionEntryPoint(ActionEntryPoint):
    max_entries_export_limit: int = Field(
        default=100000,
        gt=0,
        description='Maximum number of entries that can be exported in a single '
        'Export Entries action.',
    )
    export_archives_timeout: int = Field(
        default=10800,  # 2 hours
        gt=0,
        description='Timeout (in seconds) for the activity that reads '
        'and writes the output file.',
    )

    def load(self):
        from nomad.actions import Action

        from nomad_ml_workflows.actions.export_entries.activities import (
            cleanup_artifacts,
            export_dataset_to_upload,
            prepare_manifest,
        )
        from nomad_ml_workflows.actions.export_forces.activities import (
            manifest_archives_and_create_export,
            read_archives_and_create_export,
            write_export_forces_metadata_file,
        )
        from nomad_ml_workflows.actions.export_forces.workflows import (
            ForcesCreateExportWorkflow,
            ForcesExportEntriesWorkflow,
            ForcesExtractEntriesCombinedWorkflow,
            ForcesExtractEntriesWorkflow,
        )

        return Action(
            task_queue=self.task_queue,
            workflow=ForcesExportEntriesWorkflow,
            child_workflows=[
                ForcesExtractEntriesWorkflow,
                ForcesCreateExportWorkflow,
                ForcesExtractEntriesCombinedWorkflow,
            ],
            activities=[
                prepare_manifest,
                export_dataset_to_upload,
                cleanup_artifacts,
                write_export_forces_metadata_file,
                read_archives_and_create_export,
                manifest_archives_and_create_export,
            ],
        )


export_forces = ExportForcesActionEntryPoint(  # type: ignore
    name='Export MLIP Data Action',
    description='An action to search entries and export the Forces, Energy, and other properties of the atoms in an ASE-db/extxyz file.',
    task_queue=TaskQueue.CPU,
)
