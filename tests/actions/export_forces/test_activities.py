import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from nomad_ml_workflows.actions.export_entries.models import PrepareManifestInput
from nomad_ml_workflows.actions.export_forces import activities
from nomad_ml_workflows.actions.export_forces.models import (
    ForcesCreateExportWorkflowInput,
    ForcesExportDatasetMetadata,
    ForcesManifestArchiveExportInput,
    ForcesWriteMetadataFileInput,
)

ACTIVITIES = 'nomad_ml_workflows.actions.export_forces.activities'


@pytest.fixture
def artifacts_dir(tmp_path):
    directory = tmp_path / 'artifacts'
    directory.mkdir()
    with patch(
        f'{ACTIVITIES}.action_instance_artifacts_dir', return_value=str(directory)
    ):
        yield directory


@pytest.mark.asyncio
async def test_write_export_forces_metadata_file(artifacts_dir):
    metadata = ForcesExportDatasetMetadata(
        num_entries_exported=2, num_frames_exported=10, search_start_time='start'
    )

    result = await activities.write_export_forces_metadata_file(
        ForcesWriteMetadataFileInput(
            export_entries_workflow_id='wf-1', metadata=metadata
        )
    )

    metadata_path = artifacts_dir / 'metadata.json'
    assert result.file_path == metadata_path.as_posix()
    assert result.file_size == metadata_path.stat().st_size
    content = json.loads(metadata_path.read_text())
    assert set(content) == {'note', 'data', 'schema'}
    assert content['data'] == metadata.model_dump()
    assert content['data']['num_frames_exported'] == 10  # noqa: PLR2004
    assert content['schema'] == ForcesExportDatasetMetadata.model_json_schema()


def _export_input(output_file_format, max_entries=100):
    return ForcesManifestArchiveExportInput(
        manifest_data=PrepareManifestInput(
            export_entries_workflow_id='wf-1',
            user_id='user-1',
            owner='public',
            query={'results.material.elements': ['Si']},
            num_entries_user_limit=max_entries,
        ),
        export_data=ForcesCreateExportWorkflowInput(
            export_entries_workflow_id='wf-1',
            user_id='user-1',
            output_file_format=output_file_format,
            required_metadata=[],
            properties={'energy', 'forces'},
            max_frames=50,
        ),
    )


@pytest.fixture
def export_env(artifacts_dir):
    """Patches search, archive reading and atom writing with in-memory fakes."""
    manifest = [{'entry_id': f'e{i}', 'upload_id': 'up1'} for i in range(3)]
    env = SimpleNamespace(
        artifacts_dir=artifacts_dir,
        manifest=manifest,
        search=MagicMock(
            return_value=SimpleNamespace(pagination=SimpleNamespace(total=3))
        ),
        yield_archives=MagicMock(return_value=iter(['archive'])),
        generate_atoms=MagicMock(return_value=iter(['atoms'])),
        num_frames=7,
        written=[],
    )

    def fake_write_atoms(atoms, output_path, output_format):
        env.written.append((output_path, list(output_format)))
        extensions = {'extxyz': 'xyz', 'ase_db': 'db'}
        for fmt in output_format:
            stem = output_path.name
            (
                output_path.parent / f'{stem[: -len(".tmp")]}.tmp.{extensions[fmt]}'
            ).write_text(f'{fmt} data')
        (artifacts_dir / 'selected_entries.json').write_text(json.dumps(manifest))
        return env.num_frames

    env.write_atoms = MagicMock(side_effect=fake_write_atoms)
    info = SimpleNamespace(activity_type='manifest_archives_and_create_export')
    with (
        patch(f'{ACTIVITIES}.activity.info', return_value=info),
        patch(f'{ACTIVITIES}.nomad_search', env.search),
        patch(f'{ACTIVITIES}.yield_manifest_archives', env.yield_archives),
        patch(f'{ACTIVITIES}.generate_atoms_from_archives', env.generate_atoms),
        patch(f'{ACTIVITIES}.write_atoms_to_file', env.write_atoms),
        patch.object(activities.config, 'max_entries_export_limit', 1000),
    ):
        yield env


def test_manifest_archives_and_create_export_single_format(export_env):
    result = activities.manifest_archives_and_create_export(_export_input(['extxyz']))

    data_file = export_env.artifacts_dir / 'data.xyz'
    assert data_file.read_text() == 'extxyz data'
    assert not (export_env.artifacts_dir / 'data.tmp.xyz').exists()
    assert result.output_file.file_path == data_file.as_posix()
    assert result.output_file.file_size == data_file.stat().st_size
    assert result.output_file.num_entries_exported == 3  # noqa: PLR2004
    assert result.output_file.num_frames_exported == export_env.num_frames
    manifest_output = result.manifest_output
    assert manifest_output.num_entries_available == 3  # noqa: PLR2004
    assert manifest_output.num_entries_selected == 3  # noqa: PLR2004
    assert manifest_output.reached_max_entries_limit is False
    assert manifest_output.search_start_time <= manifest_output.search_end_time
    manifest_path = export_env.artifacts_dir / 'selected_entries.json'
    assert manifest_output.manifest_file.file_path == manifest_path.as_posix()
    assert manifest_output.manifest_file.file_size == manifest_path.stat().st_size


def test_manifest_archives_and_create_export_wires_pipeline(export_env):
    activities.manifest_archives_and_create_export(_export_input(['extxyz']))

    export_env.search.assert_called_once()
    search_kwargs = export_env.search.call_args.kwargs
    assert search_kwargs['user_id'] == 'user-1'
    assert search_kwargs['owner'] == 'public'
    assert search_kwargs['query'] == {'results.material.elements': ['Si']}
    assert search_kwargs['pagination'].page_size == 100  # noqa: PLR2004
    manifest_input, response, _, _ = export_env.yield_archives.call_args.args
    assert manifest_input.user_id == 'user-1'
    assert response is export_env.search.return_value
    gen_args = export_env.generate_atoms.call_args
    assert gen_args.args == (export_env.yield_archives.return_value,)
    assert gen_args.kwargs['max_frames'] == 50  # noqa: PLR2004
    assert set(gen_args.kwargs['properties']) == {'energy', 'forces'}
    output_path, output_format = export_env.written[0]
    assert output_path == export_env.artifacts_dir / 'data.tmp'
    assert output_format == ['extxyz']


def test_manifest_archives_and_create_export_multiple_formats(export_env):
    result = activities.manifest_archives_and_create_export(
        _export_input(['extxyz', 'ase_db'])
    )

    data_dir = export_env.artifacts_dir / 'data'
    assert (data_dir / 'data.xyz').read_text() == 'extxyz data'
    assert (data_dir / 'data.db').read_text() == 'ase_db data'
    assert export_env.written[0][0] == data_dir / 'data.tmp'
    assert result.output_file.file_path == data_dir.as_posix()
    expected_size = (data_dir / 'data.xyz').stat().st_size + (
        data_dir / 'data.db'
    ).stat().st_size
    assert result.output_file.file_size == expected_size


def test_manifest_archives_and_create_export_reaches_entries_limit(export_env):
    export_env.search.return_value = SimpleNamespace(
        pagination=SimpleNamespace(total=10)
    )
    export_env.manifest.extend(
        {'entry_id': f'x{i}', 'upload_id': 'up1'} for i in range(2)
    )

    result = activities.manifest_archives_and_create_export(
        _export_input(['extxyz'], max_entries=4)
    )

    assert result.manifest_output.num_entries_available == 10  # noqa: PLR2004
    assert result.manifest_output.num_entries_selected == 4  # noqa: PLR2004
    assert result.manifest_output.reached_max_entries_limit is True
    assert result.output_file.num_entries_exported == 4  # noqa: PLR2004


def test_manifest_archives_and_create_export_uses_config_limit(export_env):
    export_env.search.return_value = SimpleNamespace(
        pagination=SimpleNamespace(total=3)
    )

    with patch.object(activities.config, 'max_entries_export_limit', 2):
        result = activities.manifest_archives_and_create_export(
            _export_input(['extxyz'], max_entries=100)
        )

    assert export_env.search.call_args.kwargs['pagination'].page_size == 2  # noqa: PLR2004
    assert result.manifest_output.num_entries_selected == 2  # noqa: PLR2004
    assert result.manifest_output.reached_max_entries_limit is True


def test_manifest_archives_and_create_export_missing_temp_file(export_env):
    def write_nothing(atoms, output_path, output_format):
        (export_env.artifacts_dir / 'selected_entries.json').write_text('[]')
        return 0

    export_env.write_atoms.side_effect = write_nothing

    with patch(f'{ACTIVITIES}.logger') as mock_logger:
        with pytest.raises(FileNotFoundError):
            activities.manifest_archives_and_create_export(_export_input(['extxyz']))

    mock_logger.error.assert_called_once()
    assert 'data.tmp.xyz' in mock_logger.error.call_args.args[0]


def test_manifest_archives_and_create_export_available_equals_limit(export_env):
    export_env.search.return_value = SimpleNamespace(
        pagination=SimpleNamespace(total=3)
    )

    result = activities.manifest_archives_and_create_export(
        _export_input(['extxyz'], max_entries=3)
    )

    assert result.manifest_output.reached_max_entries_limit is False
