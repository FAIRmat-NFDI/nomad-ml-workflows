from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from nomad_actions.actions.entries.activities import (
        consolidate_output_files,
        create_artifact_subdirectory,
        search,
    )
    from nomad_actions.actions.entries.models import (
        ConsolidateOutputFilesInput,
        CreateArtifactSubdirectoryInput,
        SearchInput,
        SearchWorkflowUserInput,
    )


@workflow.defn
class SearchWorkflow:
    @workflow.run
    async def run(self, data: SearchWorkflowUserInput) -> str:
        retry_policy = RetryPolicy(
            maximum_attempts=3,
            initial_interval=timedelta(seconds=10),
            maximum_interval=timedelta(minutes=1),
            backoff_coefficient=2.0,
        )
        artifact_subdirectory = await workflow.execute_activity(
            create_artifact_subdirectory,
            CreateArtifactSubdirectoryInput(subdir_name=workflow.info().workflow_id),
            start_to_close_timeout=timedelta(minutes=1),
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
        return consolidated_file_path
