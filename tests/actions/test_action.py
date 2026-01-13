import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from nomad_actions.actions.entries.activities import (
    cleanup_artifacts,
    consolidate_output_files,
    create_artifact_subdirectory,
    export_dataset_to_upload,
    search,
)
from nomad_actions.actions.entries.models import ExportEntriesUserInput
from nomad_actions.actions.entries.workflows import ExportEntriesWorkflow


@pytest.mark.asyncio
async def test_simple_workflow():
    task_queue = 'test-simple-workflow'
    async with await WorkflowEnvironment.start_local() as env:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[ExportEntriesWorkflow],
            activities=[
                create_artifact_subdirectory,
                search,
                consolidate_output_files,
                export_dataset_to_upload,
                cleanup_artifacts,
            ],
        ):
            result = await env.client.execute_workflow(
                ExportEntriesWorkflow.run,
                ExportEntriesUserInput(
                    upload_id='upload_id',
                    user_id='user_id',
                    query={},
                ),
                id='test-workflow',
                task_queue=task_queue,
            )
            assert (
                result == 'hello World - created by user user_id for upload upload_id'
            )
