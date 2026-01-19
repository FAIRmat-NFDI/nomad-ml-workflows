from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from nomad.config import config as nomad_config

    from nomad_actions.actions.entries.activities import (
        cleanup_artifacts,
        create_artifact_subdirectory,
        export_dataset_to_upload,
        merge_output_files,
        search,
    )
    from nomad_actions.actions.entries.models import (
        CleanupArtifactsInput,
        CreateArtifactSubdirectoryInput,
        ExportDatasetInput,
        ExportDatasetMetadata,
        ExportEntriesUserInput,
        MergeOutputFilesInput,
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
        config = nomad_config.get_plugin_entry_point(
            'nomad_actions.actions:export_entries_action_entry_point'
        )

        artifact_subdirectory = await workflow.execute_activity(
            create_artifact_subdirectory,
            CreateArtifactSubdirectoryInput(subdir_name=workflow.info().workflow_id),
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=retry_policy,
        )

        search_counter = 0
        generated_file_paths = []
        search_start_times = []
        search_end_times = []
        total_num_entries_exported = 0
        reached_max_entries_limit = False
        search_input = SearchInput.from_user_input(
            data,
            output_file_path='',  # Placeholder, will be set in loop
            max_entries_export_limit=config.max_entries_export_limit,
        )
        while True:
            search_counter += 1
            search_input.output_file_path = (
                f'{artifact_subdirectory}/'
                f'{search_counter}.{data.output_settings.output_file_type}'
            )
            search_output = await workflow.execute_activity(
                search,
                search_input,
                activity_id=f'search-activity-{search_counter}',
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=retry_policy,
            )
            if search_output.num_entries_exported > 0:
                # only save paths if the writing files was not skipped
                generated_file_paths.append(search_input.output_file_path)
            search_start_times.append(search_output.search_start_time)
            search_end_times.append(search_output.search_end_time)
            total_num_entries_exported += search_output.num_entries_exported
            # Update pagination for next iteration
            search_input.pagination.page_after_value = (
                search_output.pagination_next_page_after_value
            )
            search_input.max_entries_export_limit -= search_output.num_entries_exported

            if search_input.max_entries_export_limit <= 0:
                # break early if the max entries limit has been reached
                reached_max_entries_limit = True
                break
            if search_output.pagination_next_page_after_value is None:
                # break if there are no more pages to fetch
                break

        if data.output_settings.merge_output_files:
            merged_file_path = await workflow.execute_activity(
                merge_output_files,
                MergeOutputFilesInput(
                    artifact_subdirectory=artifact_subdirectory,
                    output_file_type=data.output_settings.output_file_type,
                    generated_file_paths=generated_file_paths,
                ),
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=retry_policy,
            )
            if merged_file_path:
                generated_file_paths = [merged_file_path]

        saved_dataset_path = await workflow.execute_activity(
            export_dataset_to_upload,
            ExportDatasetInput(
                artifact_subdirectory=artifact_subdirectory,
                source_paths=generated_file_paths,
                metadata=ExportDatasetMetadata(
                    num_entries_exported=total_num_entries_exported,
                    num_entries_available=search_output.num_entries_available,
                    reached_max_entries=reached_max_entries_limit,
                    search_start_time=search_start_times[0]
                    if search_start_times
                    else '',
                    search_end_time=search_end_times[-1] if search_end_times else '',
                    user_input=data,
                ),
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
