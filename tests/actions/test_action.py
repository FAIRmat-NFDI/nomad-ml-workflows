import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from nomad_ml_workflows.actions.entries.activities import (
    cleanup_artifacts,
    create_artifact_subdirectory,
    export_dataset_to_upload,
    merge_output_files,
    search,
)
from nomad_ml_workflows.actions.entries.models import (
    ExportEntriesUserInput,
    SearchSettings,
)
from nomad_ml_workflows.actions.entries.workflows import ExportEntriesWorkflow


@pytest.mark.skip(reason='Requires infra support like DB connection.')
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
                merge_output_files,
                export_dataset_to_upload,
                cleanup_artifacts,
            ],
        ):
            result = await env.client.execute_workflow(
                ExportEntriesWorkflow.run,
                ExportEntriesUserInput(
                    upload_id='upload_id',
                    user_id='user_id',
                    search_settings=SearchSettings(query='{}'),
                    output_file_type='parquet',
                ),
                id='test-workflow',
                task_queue=task_queue,
            )
            assert result is not None
