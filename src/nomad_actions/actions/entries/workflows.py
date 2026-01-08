from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from nomad_actions.actions.entries.activities import (
        cleanup_artifacts,
        consolidate_output_files,
        create_artifact_subdirectory,
        save_dataset,
        search,
    )
    from nomad_actions.actions.entries.models import (
        CleanupArtifactsInput,
        ConsolidateOutputFilesInput,
        CreateArtifactSubdirectoryInput,
        SaveDatasetInput,
        SearchInput,
        SearchWorkflowUserInput,
    )


@workflow.defn
class SearchWorkflow:
    @workflow.run
    async def run(self, data: SearchWorkflowUserInput) -> str:
        """
        Workflow to perform a search action and save the results as a dataset in
        the specified upload.

        Args:
            data (SearchWorkflowUserInput): Input data for the search workflow.

        Returns:
            str: Path to the saved dataset in the upload's `raw` folder.
        """
        retry_policy = RetryPolicy(
            maximum_attempts=3,
            initial_interval=timedelta(seconds=10),
            maximum_interval=timedelta(minutes=1),
            backoff_coefficient=2.0,
        )
        artifact_subdirectory = await workflow.execute_activity(
            create_artifact_subdirectory,
            CreateArtifactSubdirectoryInput(subdir_name=workflow.info().workflow_id),
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=retry_policy,
        )
        generated_file_paths = await workflow.execute_activity(
            search,
            SearchInput.from_user_input(data, output_dir=artifact_subdirectory),
            start_to_close_timeout=timedelta(minutes=120),
            retry_policy=retry_policy,
        )
        consolidated_file_path = await workflow.execute_activity(
            consolidate_output_files,
            ConsolidateOutputFilesInput(generated_file_paths=generated_file_paths),
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=retry_policy,
        )
        saved_dataset_path = await workflow.execute_activity(
            save_dataset,
            SaveDatasetInput(
                upload_id=data.upload_id,
                user_id=data.user_id,
                source_path=consolidated_file_path,
            ),
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=retry_policy,
        )
        await workflow.execute_activity(
            cleanup_artifacts,
            CleanupArtifactsInput(subdir_path=artifact_subdirectory),
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=retry_policy,
        )

        return saved_dataset_path
