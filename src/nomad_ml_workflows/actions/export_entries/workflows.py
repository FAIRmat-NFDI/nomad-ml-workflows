import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from nomad.app.v1.models.models import MetadataPagination
    from nomad.config import config as nomad_config

    from nomad_ml_workflows.actions.export_entries.activities import (
        cleanup_artifacts,
        collect_page_cursors,
        create_artifact_subdirectory,
        export_dataset_to_upload,
        merge_output_files,
        search,
    )
    from nomad_ml_workflows.actions.export_entries.models import (
        CleanupArtifactsInput,
        CollectCursorsInput,
        CreateArtifactSubdirectoryInput,
        ExportDatasetInput,
        ExportDatasetMetadata,
        ExportEntriesUserInput,
        MergeOutputFilesInput,
        SearchInput,
        SearchOutput,
    )


@workflow.defn
class SearchPageWorkflow:
    """
    Child workflow that executes a single search page activity.

    Each instance handles one page of results, identified by its
    page_after_value cursor. Running multiple instances concurrently
    via the parent workflow achieves parallel page fetching.
    """

    @workflow.run
    async def run(self, data: SearchInput) -> SearchOutput:
        config = nomad_config.get_plugin_entry_point(
            'nomad_ml_workflows.actions:export_entries'
        )
        retry_policy = RetryPolicy(
            maximum_attempts=1,
            initial_interval=timedelta(seconds=10),
            maximum_interval=timedelta(minutes=1),
            backoff_coefficient=2.0,
        )
        return await workflow.execute_activity(
            search,
            data,
            start_to_close_timeout=timedelta(seconds=config.search_batch_timeout),
            retry_policy=retry_policy,
        )


@workflow.defn
class ExportEntriesWorkflow:
    @workflow.run
    async def run(self, data: ExportEntriesUserInput) -> str:
        """
        Workflow to search entries and export them into a datafile in the specified
        upload.

        All search pages are fetched in parallel using child workflows. A lightweight
        cursor-collection activity first walks the pagination to determine the
        page_after_value for every page, then one SearchPageWorkflow child workflow
        is launched per page and all are awaited concurrently.

        Args:
            data (ExportEntriesUserInput): Input data for the export entries workflow.
        Returns:
            str: Path to the saved dataset in the upload's `raw` folder.
        """
        retry_policy = RetryPolicy(
            maximum_attempts=1,
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
        export_dataset_input = ExportDatasetInput(
            user_id=data.user_id,
            upload_id=data.upload_id,
            artifact_subdirectory=artifact_subdirectory,
            exportable_dir_name='export_entries_error',  # name used in case of error
            zip_output=data.output_settings.zip_output,
            source_paths=[],
            metadata=ExportDatasetMetadata(user_input=data),
        )

        try:
            config = nomad_config.get_plugin_entry_point(
                'nomad_ml_workflows.actions:export_entries'
            )

            # Build a representative SearchInput to resolve shared settings
            # (query, owner, required, batch_file_type) once.
            template_search_input = SearchInput.from_user_input(
                data,
                output_file_path='',  # placeholder, real paths are set per page below
                max_entries_export_limit=config.max_entries_export_limit,
            )
            page_size = data.search_settings.page_size

            # Collect all page cursors with a lightweight serial walk
            cursors_output = await workflow.execute_activity(
                collect_page_cursors,
                CollectCursorsInput(
                    user_id=data.user_id,
                    owner=data.search_settings.owner,
                    query=template_search_input.query,
                    page_size=page_size,
                    max_entries_export_limit=config.max_entries_export_limit,
                ),
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=retry_policy,
            )

            num_entries_available = cursors_output.num_entries_available
            reached_max_entries_limit = (
                num_entries_available > config.max_entries_export_limit
            )

            # Build one SearchInput per page with the corresponding cursor and
            # export limit for that page
            search_inputs: list[SearchInput] = []
            for page_index, cursor in enumerate(cursors_output.page_after_values):
                entries_so_far = page_index * page_size
                limit_for_page = min(
                    page_size, config.max_entries_export_limit - entries_so_far
                )
                si = template_search_input.model_copy(
                    update={
                        'output_file_path': (
                            f'{artifact_subdirectory}/{page_index + 1}'
                            f'.{template_search_input.batch_file_type}'
                        ),
                        'max_entries_export_limit': limit_for_page,
                        'pagination': MetadataPagination(
                            page_size=page_size,
                            page_after_value=cursor,
                        ),
                    }
                )
                search_inputs.append(si)

            # Run search for all pages in parallel as child workflows
            search_results = await asyncio.gather(
                *[
                    workflow.execute_child_workflow(
                        SearchPageWorkflow.run,
                        si,
                        id=f'{workflow.info().workflow_id}-search-page-{i + 1}',
                    )
                    for i, si in enumerate(search_inputs)
                ]
            )

            # Collect outputs preserving page order
            generated_file_paths = [
                search_inputs[i].output_file_path
                for i, result in enumerate(search_results)
                if result.num_entries_exported > 0
            ]
            total_num_entries_exported = sum(
                result.num_entries_exported for result in search_results
            )
            search_start_times = [result.search_start_time for result in search_results]
            search_end_times = [result.search_end_time for result in search_results]

            # Merge batch files into one file to be exported
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

            # Prepare export dataset input and metadata
            # Pages ran in parallel so take the earliest start and latest end.
            earliest_start = min(search_start_times)
            latest_end = max(search_end_times)
            export_dataset_input.exportable_dir_name = (
                'export_entries_' + earliest_start.replace(':', '-')
            )
            export_dataset_input.source_paths = [merged_file_path]
            export_dataset_input.metadata = ExportDatasetMetadata(
                num_entries_exported=total_num_entries_exported,
                num_entries_available=num_entries_available,
                reached_max_entries_limit=reached_max_entries_limit,
                search_start_time=earliest_start,
                search_end_time=latest_end,
                user_input=data,
            )

        except Exception as e:
            # Capture error info to include in metadata
            import traceback

            export_dataset_input.metadata.error_info = traceback.format_exc()
            raise ApplicationError(
                'Encountered an error during export entries workflow.',
            ) from e

        finally:
            saved_dataset_path = await workflow.execute_activity(
                export_dataset_to_upload,
                export_dataset_input,
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
