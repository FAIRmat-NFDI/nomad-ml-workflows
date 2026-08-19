"""
Temporal workflows for managing local and remote entry exports.
"""

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
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
        copy_remote_dataset_to_upload,
        read_num_entries_exported,
        resolve_export_remote_entries_runtime_activity,
        upload_dataset_to_remote_storage,
    )
    from nomad_ml_workflows.actions.export_remote_entries.models import (
        CopyRemoteDatasetToUploadInput,
        ExportRemoteDatasetInput,
        ExportRemoteEntriesOutput,
        ExportRemoteEntriesUserInput,
        OasisExecutionResult,
        ResolveExportRemoteEntriesRuntimeOutput,
    )
    from nomad_ml_workflows.actions.export_remote_entries.nexus_contract import (
        ExportRemoteEntriesService,
        RemoteExtractInput,
    )


def _select_primary_remote_uri(
    results_dict: dict[str, OasisExecutionResult],
    results_list: list[OasisExecutionResult],
) -> str:
    """Determine the primary remote URI from execution results."""
    local_res = results_dict.get('local')
    if local_res and local_res.remote_uri:
        return local_res.remote_uri

    for res in results_list:
        if res.remote_uri:
            return res.remote_uri

    return ''


async def _save_dataset_to_upload(
    data: ExportRemoteEntriesUserInput, remote_uri: str
) -> None:
    """Optionally copy the S3 dataset into the requested staging upload."""
    if not data.save_to_upload:
        return
    if not data.upload_id:
        raise ApplicationError('upload_id is required when save_to_upload is enabled.')

    await workflow.execute_activity(
        copy_remote_dataset_to_upload,
        CopyRemoteDatasetToUploadInput(
            user_id=data.user_id,
            upload_id=data.upload_id,
            remote_uri=remote_uri,
            storage_settings=data.storage_settings,
            zip_output=data.export_settings.create_zip_archive,
        ),
        start_to_close_timeout=timedelta(hours=2),
        retry_policy=RetryPolicy(maximum_attempts=1),
    )


async def _execute_local(
    data: ExportRemoteEntriesUserInput, retry_policy: RetryPolicy
) -> OasisExecutionResult:
    """Execute entry extraction and storage upload locally."""
    workflow_id = workflow.info().workflow_id
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
                export_entries_workflow_id=workflow_id,
                user_input=extract_user_input,
            ),
            id=f'{workflow_id}-extract-entries',
            parent_close_policy=workflow.ParentClosePolicy.TERMINATE,
            retry_policy=retry_policy,
        )
        timestamp_str = workflow.info().start_time.isoformat()
        remote_uri = await workflow.execute_activity(
            upload_dataset_to_remote_storage,
            ExportRemoteDatasetInput(
                export_entries_workflow_id=workflow_id,
                storage_settings=data.storage_settings,
                exportable_dir_name=f'export_entries_{timestamp_str}',
                zip_output=data.export_settings.create_zip_archive,
            ),
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=retry_policy,
        )
        await _save_dataset_to_upload(data, remote_uri)
        num_entries_exported = await workflow.execute_activity(
            read_num_entries_exported,
            workflow_id,
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=retry_policy,
        )
        return OasisExecutionResult(
            target_key='local',
            status='SUCCESS',
            is_remote=False,
            num_entries_exported=num_entries_exported,
            remote_uri=remote_uri,
        )
    except Exception as exc:
        workflow.logger.error(f'Local extraction failed: {exc}')
        raise ApplicationError(
            f'Local export failed: {exc}',
            type='RemoteExportActivityError',
        )
    finally:
        await workflow.execute_activity(
            cleanup_artifacts,
            CleanupArtifactsInput(export_entries_workflow_id=workflow_id),
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=retry_policy,
        )


async def _execute_remote_nexus(
    target_key: str,
    endpoint_name: str,
    data: ExportRemoteEntriesUserInput,
) -> OasisExecutionResult:
    """Execute entry extraction on a remote Oasis via Temporal Nexus RPC."""
    try:
        nexus_client = workflow.create_nexus_client(
            endpoint=endpoint_name,
            service=ExportRemoteEntriesService,
        )
        nexus_result: ExportRemoteEntriesOutput = await nexus_client.execute_operation(
            ExportRemoteEntriesService.export_remote_entries,
            RemoteExtractInput(
                user_id=data.user_id,
                search_settings=data.search_settings,
                export_settings=data.export_settings,
                storage_settings=data.storage_settings,
            ),
            schedule_to_close_timeout=timedelta(hours=2),
        )
        local_result = nexus_result.results.get('local')
        if local_result is None:
            raise ApplicationError(
                'Remote export completed without a local execution result.'
            )
        if local_result.status != 'SUCCESS':
            raise ApplicationError(
                local_result.error_message or 'Remote export failed.'
            )
        if not local_result.remote_uri:
            raise ApplicationError('Remote export completed without an S3 URI.')
        await _save_dataset_to_upload(data, local_result.remote_uri)
        return OasisExecutionResult(
            target_key=target_key,
            status=local_result.status,
            is_remote=True,
            num_entries_exported=local_result.num_entries_exported,
            remote_uri=local_result.remote_uri,
            error_message=local_result.error_message,
        )
    except Exception as exc:
        workflow.logger.error(
            f'Remote Nexus extraction for target "{target_key}" failed: {exc}'
        )
        return OasisExecutionResult(
            target_key=target_key,
            status='FAILED',
            is_remote=True,
            num_entries_exported=0,
            remote_uri=endpoint_name,
            error_message=str(exc),
        )


@workflow.defn
class ExportRemoteEntriesWorkflow:
    """Workflow entry point for the action's aggregate result."""

    @staticmethod
    def _normalize_s3_storage_input(
        data: ExportRemoteEntriesUserInput,
        runtime: ResolveExportRemoteEntriesRuntimeOutput,
    ) -> ExportRemoteEntriesUserInput:
        """Validate and normalize S3 storage settings based on runtime s3_mode."""
        if runtime.s3_mode == 'workflow_input':
            if data.storage_settings is None:
                raise ApplicationError(
                    'S3 storage settings are required when export_remote_entries s3_mode is `workflow_input`.'
                )
            return data

        if runtime.resolved_storage_settings is None:
            raise ApplicationError(
                'S3 storage settings could not be resolved from entrypoint or environment.'
            )
        return data.model_copy(
            update={'storage_settings': runtime.resolved_storage_settings}
        )

    @workflow.run
    async def run(
        self, data: ExportRemoteEntriesUserInput
    ) -> ExportRemoteEntriesOutput:
        """Extract matching entries across the requested Oases."""
        start_time = workflow.time()
        retry_policy = RetryPolicy(maximum_attempts=1)

        runtime: ResolveExportRemoteEntriesRuntimeOutput = (
            await workflow.execute_activity(
                resolve_export_remote_entries_runtime_activity,
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=retry_policy,
            )
        )
        data = self._normalize_s3_storage_input(data, runtime)

        tasks = []
        for target in data.target_oases:
            if target == 'local':
                tasks.append(_execute_local(data, retry_policy))
            else:
                tasks.append(
                    _execute_remote_nexus(
                        target_key=target,
                        endpoint_name=target,
                        data=data,
                    )
                )

        results_list: list[OasisExecutionResult] = await asyncio.gather(*tasks)
        results_dict = {res.target_key: res for res in results_list}
        total_exported = sum(res.num_entries_exported for res in results_list)

        primary_uri = _select_primary_remote_uri(results_dict, results_list)

        return ExportRemoteEntriesOutput(
            results=results_dict,
            total_entries_exported=total_exported,
            remote_uri=primary_uri,
            workflow_duration=round(workflow.time() - start_time, 6),
        )
