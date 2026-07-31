from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from nomad.config import config as nomad_config

    from nomad_ml_workflows.actions.export_entries.activities import (
        cleanup_artifacts,
        create_artifact_subdirectory,
        export_dataset_to_upload,
        prepare_manifest,
        read_archives_and_write_output_json,
        read_archives_and_write_output_tabular,
    )
    from nomad_ml_workflows.actions.export_entries.models import (
        CleanupArtifactsInput,
        CreateArtifactSubdirectoryInput,
        ExportDatasetInput,
        ExportDatasetMetadata,
        ExportEntriesOutput,
        ExportEntriesUserInput,
        NormalizedSearchSettings,
        OutputFile,
        PrepareManifestInput,
        ReadArchivesWorkflowInput,
    )

config = nomad_config.get_plugin_entry_point(
    'nomad_ml_workflows.actions:export_entries'
)


@workflow.defn
class ReadArchivesWorkflow:
    """
    Child workflow that reads an archive and writes the output file.
    """

    @workflow.run
    async def run(self, data: ReadArchivesWorkflowInput) -> OutputFile:
        retry_policy = RetryPolicy(maximum_attempts=1)

        if data.output_file_format == 'json':
            return await workflow.execute_activity(
                read_archives_and_write_output_json,
                data,
                start_to_close_timeout=timedelta(seconds=config.read_archives_timeout),  # type: ignore
                retry_policy=retry_policy,
            )

        if data.output_file_format in ['parquet', 'csv']:
            return await workflow.execute_activity(
                read_archives_and_write_output_tabular,
                data,
                start_to_close_timeout=timedelta(seconds=config.read_archives_timeout),  # type: ignore
                retry_policy=retry_policy,
            )

        raise ValueError(f'Unsupported output file format: {data.output_file_format}')


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
            source_paths=None,
            metadata=ExportDatasetMetadata(user_input=data),  # type: ignore
        )

        try:
            search_settings = NormalizedSearchSettings.from_user_input(data)

            manifest_file_path = f'{artifact_subdirectory}/manifest.json'
            manifest_output = await workflow.execute_activity(
                prepare_manifest,
                PrepareManifestInput(
                    user_id=search_settings.user_id,
                    owner=search_settings.owner,
                    query=search_settings.query,
                    num_entries_user_limit=search_settings.num_entries_user_limit,  # type: ignore
                    manifest_file_path=manifest_file_path,
                ),
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=retry_policy,
            )

            export_dataset_input.metadata.reached_max_entries_limit = (
                manifest_output.reached_max_entries_limit
            )
            export_dataset_input.metadata.num_entries_available = (
                manifest_output.num_entries_available
            )
            if manifest_output.num_entries_available == 0:
                # No pages to export, return early with an empty dataset
                return ExportEntriesOutput(exported_dir_path='', workflow_duration=0.0)

            output_file: OutputFile = await workflow.execute_child_workflow(
                ReadArchivesWorkflow.run,
                ReadArchivesWorkflowInput(
                    user_id=data.user_id,
                    output_file_format=data.output_settings.output_file_format,
                    manifest_file_path=manifest_file_path,
                    artifact_subdirectory=artifact_subdirectory,
                    required=search_settings.required,
                ),
                id=f'{workflow.info().workflow_id}-read-archives-and-write-file',
                parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
                retry_policy=retry_policy,
            )

            export_dataset_input.metadata.num_entries_exported = (
                output_file.num_entries_exported
            )
            export_dataset_input.metadata.search_start_time = (
                manifest_output.search_start_time
            )
            export_dataset_input.metadata.search_end_time = (
                manifest_output.search_end_time
            )

            export_dataset_input.source_paths = [
                manifest_file_path,
                output_file.file_path,
            ]

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
