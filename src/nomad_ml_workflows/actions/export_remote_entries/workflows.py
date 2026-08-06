from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from nomad.config import config as nomad_config

    from nomad_ml_workflows.actions.export_entries.activities import (
        cleanup_artifacts,
    )
    from nomad_ml_workflows.actions.export_entries.models import (
        CleanupArtifactsInput,
        ExportEntriesUserInput,
        ExtractEntriesWorkflowInput,
    )
    from nomad_ml_workflows.actions.export_entries.workflows import (
        ExtractEntriesWorkflow,
    )
    from nomad_ml_workflows.actions.export_remote_entries.activities import (
        upload_dataset_to_remote_storage,
    )
    from nomad_ml_workflows.actions.export_remote_entries.models import (
        ExportRemoteDatasetInput,
        ExportRemoteEntriesOutput,
        ExportRemoteEntriesUserInput,
    )

config = nomad_config.get_plugin_entry_point(
    'nomad_ml_workflows.actions:export_remote_entries'
)


@workflow.defn
class ExportRemoteEntriesWorkflow:
    @workflow.run
    async def run(
        self, data: ExportRemoteEntriesUserInput
    ) -> ExportRemoteEntriesOutput:
        """
        Extract matching entries and export generated files to remote storage.
        """
        starttime = workflow.time()
        retry_policy = RetryPolicy(maximum_attempts=1)

        extract_user_input = ExportEntriesUserInput(
            user_id=data.user_id,
            upload_id='',
            search_settings=data.search_settings,
            export_settings=data.export_settings,
        )

        try:
            await workflow.execute_child_workflow(
                ExtractEntriesWorkflow.run,
                ExtractEntriesWorkflowInput(
                    export_entries_workflow_id=workflow.info().workflow_id,
                    user_input=extract_user_input,
                ),
                id=f'{workflow.info().workflow_id}-extract-entries',
                parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
                retry_policy=retry_policy,
            )
        except Exception as e:
            raise ApplicationError(
                'Encountered an error during extract entries workflow.',
            ) from e
        finally:
            remote_uri = await workflow.execute_activity(
                upload_dataset_to_remote_storage,
                ExportRemoteDatasetInput(
                    export_entries_workflow_id=workflow.info().workflow_id,
                    storage_settings=data.storage_settings,
                    exportable_dir_name=(
                        f'export_entries_{workflow.info().start_time.isoformat()}'
                    ),
                    zip_output=data.export_settings.create_zip_archive,
                ),
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=retry_policy,
            )

        await workflow.execute_activity(
            cleanup_artifacts,
            CleanupArtifactsInput(
                export_entries_workflow_id=workflow.info().workflow_id
            ),
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=retry_policy,
        )

        return ExportRemoteEntriesOutput(
            remote_uri=remote_uri,
            workflow_duration=round(workflow.time() - starttime, 6),
        )
