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
        read_archives,
    )
    from nomad_ml_workflows.actions.export_entries.models import (
        CleanupArtifactsInput,
        CollectCursorsInput,
        CreateArtifactSubdirectoryInput,
        ExportDatasetInput,
        ExportDatasetMetadata,
        ExportEntriesOutput,
        ExportEntriesUserInput,
        MergeOutputFilesInput,
        ReadArchivesInput,
        SearchPageInput,
        SearchPageOutput,
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
    async def run(self, data: SearchPageInput) -> SearchPageOutput:
        config = nomad_config.get_plugin_entry_point(
            'nomad_ml_workflows.actions:export_entries'
        )
        retry_policy = RetryPolicy(maximum_attempts=1)

        rai = ReadArchivesInput.from_search_page_input(data)
        return await workflow.execute_activity(
            read_archives,
            rai,
            start_to_close_timeout=timedelta(seconds=config.search_batch_timeout),
            retry_policy=retry_policy,
        )


@workflow.defn
class ExportEntriesWorkflow:
    @workflow.run
    async def run(self, data: ExportEntriesUserInput) -> ExportEntriesOutput:
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
        starttime = workflow.time()
        retry_policy = RetryPolicy(maximum_attempts=1)
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
            exportable_dir_name=(
                f'export_entries_{workflow.info().start_time.isoformat()}'
            ),
            zip_output=data.output_settings.zip_output,
            source_paths=[],
            metadata=ExportDatasetMetadata(user_input=data),
        )

        try:
            config = nomad_config.get_plugin_entry_point(
                'nomad_ml_workflows.actions:export_entries'
            )

            # Build a representative SearchPageInput to resolve shared settings
            # (query, owner, required, batch_file_format) once.
            template_spi = SearchPageInput.from_user_input(
                data,
                page_num=0,
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
                    query=template_spi.query,
                    page_size=page_size,
                    max_entries_export_limit=config.max_entries_export_limit,
                ),
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=retry_policy,
            )
            export_dataset_input.metadata.reached_max_entries_limit = (
                cursors_output.num_entries_available > config.max_entries_export_limit
            )
            export_dataset_input.metadata.num_entries_available = (
                cursors_output.num_entries_available
            )
            if cursors_output.num_pages == 0:
                # No pages to export, return early with an empty dataset
                return ExportEntriesOutput(exported_dir_path='', workflow_duration=0.0)

            # Build one SearchPageInput per page with the corresponding cursor and
            # export limit for that page
            search_page_inputs: list[SearchPageInput] = []
            for page_iter, cursor in enumerate(cursors_output.page_after_values):
                page_num = page_iter + 1
                entries_so_far = page_iter * page_size
                limit_for_page = min(
                    page_size, config.max_entries_export_limit - entries_so_far
                )
                spi = template_spi.model_copy(
                    update={
                        'page_num': page_num,
                        'output_file_path': (
                            f'{artifact_subdirectory}/{page_num}'
                            f'.{template_spi.batch_file_format}'
                        ),
                        'max_entries_export_limit': limit_for_page,
                        'pagination': MetadataPagination(
                            page_size=page_size,
                            page_after_value=cursor,
                        ),
                    }
                )
                search_page_inputs.append(spi)

            # Run child workflows for each page with bounded concurrency to avoid
            # overwhelming the Temporal server with too many concurrent workflows.
            search_page_outputs: list[SearchPageOutput] = []
            for concurr_batch_start in range(
                0, len(search_page_inputs), config.search_workflow_concurrency_limit
            ):
                concurr_batch_spis = search_page_inputs[
                    concurr_batch_start : concurr_batch_start
                    + config.search_workflow_concurrency_limit
                ]
                concurr_batch_spos = await asyncio.gather(
                    *[
                        workflow.execute_child_workflow(
                            SearchPageWorkflow.run,
                            spi,
                            id=f'{workflow.info().workflow_id}-search-page-'
                            f'{spi.page_num}',
                            parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
                            retry_policy=retry_policy,
                        )
                        for spi in concurr_batch_spis
                    ]
                )
                search_page_outputs.extend(concurr_batch_spos)

            # Pages ran concurrently so take the earliest start and latest end. ISO
            # timestamp strings can be compared lexicographically
            export_dataset_input.metadata.num_entries_exported = sum(
                spo.num_entries_exported for spo in search_page_outputs
            )
            export_dataset_input.metadata.search_start_time = min(
                [spo.search_start_time for spo in search_page_outputs]
            )
            export_dataset_input.metadata.search_end_time = max(
                [spo.search_end_time for spo in search_page_outputs]
            )

            # Merge batch files into one file to be exported
            # Only include paths where entries were actually written to disk
            generated_file_paths = [
                search_page_inputs[i].output_file_path
                for i, spo in enumerate(search_page_outputs)
                if spo.num_entries_exported > 0
            ]
            if generated_file_paths:
                merged_file_path = await workflow.execute_activity(
                    merge_output_files,
                    MergeOutputFilesInput(
                        artifact_subdirectory=artifact_subdirectory,
                        output_file_format=data.output_settings.output_file_format,
                        generated_file_paths=generated_file_paths,
                    ),
                    start_to_close_timeout=timedelta(hours=2),
                    retry_policy=retry_policy,
                )
                export_dataset_input.source_paths = [merged_file_path]

        except Exception as e:
            # Capture error info to include in metadata
            import traceback

            export_dataset_input.metadata.error_info = traceback.format_exc()
            raise ApplicationError(
                'Encountered an error during export entries workflow.',
            ) from e

        finally:
            exported_dir_path = await workflow.execute_activity(
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

            output = ExportEntriesOutput(
                exported_dir_path=exported_dir_path,
                workflow_duration=round(workflow.time() - starttime, 6),
            )

        return output
