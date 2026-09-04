from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from nomad.config import config as nomad_config

    from nomad_ml_workflows import __version__ as nomad_ml_workflows_version
    from nomad_ml_workflows.actions.export_atoms.activities import (
        atoms_write_metadata_file,
        read_archives_and_generate_atoms,
    )
    from nomad_ml_workflows.actions.export_atoms.models import (
        AtomsExportDatasetMetadata,
        AtomsExportEntriesUserInput,
        AtomsExtractEntriesWorkflowInput,
        AtomsNormalizedSearchSettings,
        AtomsReadArchivesWorkflowInput,
        AtomsWriteMetadataFileInput,
    )
    from nomad_ml_workflows.actions.export_entries.activities import (
        cleanup_artifacts,
        export_dataset_to_upload,
        prepare_manifest,
    )
    from nomad_ml_workflows.actions.export_entries.models import (
        CleanupArtifactsInput,
        ExportDatasetInput,
        ExportEntriesOutput,
        ExtractEntriesWorkflowOutput,
        OutputFile,
        PrepareManifestInput,
    )

config = nomad_config.get_plugin_entry_point(
    'nomad_ml_workflows.actions:export_entries'
)


@workflow.defn
class AtomsReadArchivesWorkflow:
    """
    Child workflow that reads archives and writes the output artifact.
    """

    @workflow.run
    async def run(self, data: AtomsReadArchivesWorkflowInput) -> OutputFile:
        retry_policy = RetryPolicy(maximum_attempts=1)
        return await workflow.execute_activity(
            read_archives_and_generate_atoms,
            data,
            start_to_close_timeout=timedelta(seconds=config.read_archives_timeout),  # type: ignore
            retry_policy=retry_policy,
        )


@workflow.defn
class AtomsExtractEntriesWorkflow:
    @workflow.run
    async def run(
        self, data: AtomsExtractEntriesWorkflowInput
    ) -> ExtractEntriesWorkflowOutput:
        """
        Find matching entries and write their archives to action artifact subdirectory.
        """
        retry_policy = RetryPolicy(maximum_attempts=1)
        user_input = data.user_input
        metadata = AtomsExportDatasetMetadata(
            user_input=user_input,
            nomad_deployment_api_host=nomad_config.services.api_host,
            nomad_version=nomad_config.meta.version,
            nomad_ml_workflows_version=nomad_ml_workflows_version,
        )  # type: ignore
        workflow_output = ExtractEntriesWorkflowOutput()  # type: ignore

        try:
            search_settings = AtomsNormalizedSearchSettings.from_user_input(user_input)
            manifest_output = await workflow.execute_activity(
                prepare_manifest,
                PrepareManifestInput(
                    export_entries_workflow_id=data.export_entries_workflow_id,
                    user_id=search_settings.user_id,
                    owner=search_settings.owner,
                    query=search_settings.query,
                    num_entries_user_limit=search_settings.num_entries_user_limit,
                ),
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=retry_policy,
            )
            workflow_output.manifest_file_path = manifest_output.manifest_file.file_path

            metadata.num_entries_available = manifest_output.num_entries_available
            metadata.num_entries_selected = manifest_output.num_entries_selected
            metadata.reached_max_entries_limit = (
                manifest_output.reached_max_entries_limit
            )
            metadata.search_start_time = manifest_output.search_start_time
            metadata.search_end_time = manifest_output.search_end_time

            if manifest_output.num_entries_selected > 0:
                output_file: OutputFile = await workflow.execute_child_workflow(
                    AtomsReadArchivesWorkflow.run,
                    AtomsReadArchivesWorkflowInput(
                        export_entries_workflow_id=data.export_entries_workflow_id,
                        user_id=user_input.user_id,
                        output_file_format=user_input.export_settings.file_format,
                        properties=user_input.search_settings.required_properties,
                    ),
                    id=f'{workflow.info().workflow_id}-read-archives-and-write-file',
                    parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
                    retry_policy=retry_policy,
                )
                metadata.num_entries_exported = output_file.num_entries_exported
                workflow_output.data_file_path = output_file.file_path

        except Exception as e:
            # Add error info to metadata and re-raise
            import traceback

            metadata.error_info = traceback.format_exc()

            raise ApplicationError(
                'Encountered an error during reading archives and writing the data '
                'artifact.',
            ) from e

        finally:
            metadata_file = await workflow.execute_activity(
                atoms_write_metadata_file,
                AtomsWriteMetadataFileInput(
                    export_entries_workflow_id=data.export_entries_workflow_id,
                    metadata=metadata,
                ),
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=retry_policy,
            )
            workflow_output.metadata_file_path = metadata_file.file_path

        return workflow_output


@workflow.defn
class AtomsExportEntriesWorkflow:
    @workflow.run
    async def run(self, data: AtomsExportEntriesUserInput) -> ExportEntriesOutput:
        """
        Extract matching entries and export the generated files to an upload.
        """
        starttime = workflow.time()
        retry_policy = RetryPolicy(maximum_attempts=1)

        try:
            await workflow.execute_child_workflow(
                AtomsExtractEntriesWorkflow.run,
                AtomsExtractEntriesWorkflowInput(
                    export_entries_workflow_id=workflow.info().workflow_id,
                    user_input=data,
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
            # Always export artifacts once extraction workflow is triggered
            exported_dir_path = await workflow.execute_activity(
                export_dataset_to_upload,
                ExportDatasetInput(
                    export_entries_workflow_id=workflow.info().workflow_id,
                    user_id=data.user_id,
                    upload_id=data.upload_id,
                    exportable_dir_name=(
                        f'export_entries_{workflow.info().start_time.isoformat()}'
                    ),
                    zip_output=data.export_settings.create_zip_archive,
                ),
                start_to_close_timeout=timedelta(hours=2),
                retry_policy=retry_policy,
            )

        # Cleanup artifacts only if extraction workflow succeeded
        await workflow.execute_activity(
            cleanup_artifacts,
            CleanupArtifactsInput(
                export_entries_workflow_id=workflow.info().workflow_id
            ),
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=retry_policy,
        )

        return ExportEntriesOutput(
            exported_dir_path=exported_dir_path,
            workflow_duration=round(workflow.time() - starttime, 6),
        )
