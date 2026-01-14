from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from nomad_actions.actions.entries.activities import (
        cleanup_artifacts,
        consolidate_output_files,
        create_artifact_subdirectory,
        export_dataset_to_upload,
        search,
    )
    from nomad_actions.actions.entries.models import (
        CleanupArtifactsInput,
        ConsolidateOutputFilesInput,
        CreateArtifactSubdirectoryInput,
        ExportDatasetInput,
        ExportEntriesUserInput,
        SearchInput,
    )


@workflow.defn
class ExportEntriesWorkflow:
    @workflow.run
    async def run(self, data: ExportEntriesUserInput) -> str:
        """
        Workflow to search entries and export them into a datafile in the specified
        upload.

        Args:
            data (ExportEntriesUserInput): Input data for the export entries workflow.
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

        search_counter = 0
        generated_file_paths = []
        search_input = SearchInput.from_user_input(
            data,
            output_file_path='',  # Placeholder, will be set in loop
        )
        while True:
            search_counter += 1
            search_input.output_file_path = (
                f'{artifact_subdirectory}/{search_counter}.{data.output_file_type}'
            )
            search_output = await workflow.execute_activity(
                search,
                search_input,
                activity_id=f'search-activity-{search_counter}',
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=retry_policy,
            )
            generated_file_paths.append(search_input.output_file_path)
            if search_output.pagination_next_page_after_value is None:
                break
            # Update pagination for next iteration
            search_input.pagination.page_after_value = (
                search_output.pagination_next_page_after_value
            )

        consolidated_file_path = await workflow.execute_activity(
            consolidate_output_files,
            ConsolidateOutputFilesInput(generated_file_paths=generated_file_paths),
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=retry_policy,
        )

        saved_dataset_path = await workflow.execute_activity(
            export_dataset_to_upload,
            ExportDatasetInput(
                upload_id=data.upload_id,
                user_id=data.user_id,
                source_path=consolidated_file_path,
            ),
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=retry_policy,
        )

        await workflow.execute_activity(
            cleanup_artifacts,
            CleanupArtifactsInput(subdir_path=artifact_subdirectory),
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=retry_policy,
        )

        return saved_dataset_path
