import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from nomad_ml_workflows.actions.export_entries.models import PrepareManifestInput
from nomad_ml_workflows.actions.export_forces import utils

UTILS = 'nomad_ml_workflows.actions.export_forces.utils'


def test_generate_atoms_from_archives_forwards_arguments():
    atoms_generator = MagicMock(return_value=iter(['atoms']))
    archives = iter([])

    with patch(
        f'{UTILS}.require_nomad_forces_export', return_value=(atoms_generator, None)
    ):
        result = utils.generate_atoms_from_archives(
            archives, properties=['energy', 'forces', 'energy'], max_frames=7
        )

    assert list(result) == ['atoms']
    atoms_generator.assert_called_once_with(
        archives, properties={'energy', 'forces'}, max_frames=7
    )


def test_generate_atoms_from_archives_max_frames_defaults_to_none():
    atoms_generator = MagicMock()

    with patch(
        f'{UTILS}.require_nomad_forces_export', return_value=(atoms_generator, None)
    ):
        utils.generate_atoms_from_archives(iter([]), properties=['energy'])

    assert atoms_generator.call_args.kwargs['max_frames'] is None


def test_write_atoms_to_file_forwards_arguments(tmp_path):
    write_atoms = MagicMock(return_value=3)
    atoms = iter([])
    output_path = tmp_path / 'data.tmp'

    with patch(
        f'{UTILS}.require_nomad_forces_export', return_value=(None, write_atoms)
    ):
        frames = utils.write_atoms_to_file(
            atoms, output_path, output_format=['extxyz', 'ase_db']
        )

    assert frames == 3  # noqa: PLR2004
    write_atoms.assert_called_once_with(
        atoms, output_path=output_path, output_format=['extxyz', 'ase_db']
    )


def test_write_atoms_to_file_defaults_to_extxyz(tmp_path):
    write_atoms = MagicMock(return_value=0)

    with patch(
        f'{UTILS}.require_nomad_forces_export', return_value=(None, write_atoms)
    ):
        utils.write_atoms_to_file(iter([]), tmp_path / 'data.tmp')

    assert write_atoms.call_args.kwargs['output_format'] == ['extxyz']


class FakeUploadFiles:
    """Stands in for nomad UploadFiles, backed by a dict of entry_id -> archive."""

    def __init__(self, archives: dict, failing: set | None = None):
        self.archives = archives
        self.failing = failing or set()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    @contextmanager
    def read_archive(self, entry_id):
        if entry_id in self.failing:
            raise RuntimeError(f'cannot read {entry_id}')
        yield self.archives[entry_id]


class FakeReader:
    """Replaces RequiredReader; returns the stored archive for each entry."""

    def __init__(self, *args, **kwargs):
        pass

    def read(self, upload_archive, entry_id, upload_id):
        return upload_archive


def _entries(*pairs):
    return [{'upload_id': u, 'entry_id': e} for u, e in pairs]


def _response(entries, next_page_after_value=None):
    return SimpleNamespace(
        data=entries,
        pagination=SimpleNamespace(next_page_after_value=next_page_after_value),
    )


def _manifest_input(limit=100, query=None):
    return PrepareManifestInput(
        export_entries_workflow_id='wf-1',
        user_id='user-1',
        owner='public',
        query=query or {},
        num_entries_user_limit=limit,
    )


@pytest.fixture
def manifest_env(tmp_path):
    """Patches every external dependency of ``yield_manifest_archives``."""
    artifacts = tmp_path / 'artifacts'
    artifacts.mkdir()
    uploads: dict[str, FakeUploadFiles | None] = {}
    env = SimpleNamespace(
        artifacts=artifacts,
        manifest_path=artifacts / f'{utils.MANIFEST_FILE_NAME}.json',
        uploads=uploads,
        search=MagicMock(),
        archive_filter=MagicMock(return_value=False),
    )
    with (
        patch(f'{UTILS}.action_instance_artifacts_dir', return_value=str(artifacts)),
        patch(f'{UTILS}.UploadFiles.get', side_effect=uploads.get),
        patch(f'{UTILS}.RequiredReader', FakeReader),
        patch(f'{UTILS}.nomad_search', env.search),
        patch(
            f'{UTILS}.require_nomad_force_export_config',
            return_value=({}, env.archive_filter),
        ),
        patch.object(utils.config, 'max_entries_export_limit', 1000),
    ):
        yield env


def _read_manifest(env):
    return json.loads(env.manifest_path.read_text())


def test_yield_manifest_archives_yields_entries_and_writes_manifest(manifest_env):
    manifest_env.uploads['up1'] = FakeUploadFiles({'e1': {'a': 1}, 'e2': {'a': 2}})
    response = _response(_entries(('up1', 'e1'), ('up1', 'e2')))

    results = list(utils.yield_manifest_archives(_manifest_input(), response))

    assert results == [
        {'entry_id': 'e1', 'upload_id': 'up1', 'archive': {'a': 1}},
        {'entry_id': 'e2', 'upload_id': 'up1', 'archive': {'a': 2}},
    ]
    assert _read_manifest(manifest_env) == [
        {'entry_id': 'e1', 'upload_id': 'up1'},
        {'entry_id': 'e2', 'upload_id': 'up1'},
    ]
    manifest_env.search.assert_not_called()


def test_yield_manifest_archives_groups_entries_by_upload(manifest_env):
    manifest_env.uploads['up1'] = FakeUploadFiles({'e1': {}, 'e3': {}})
    manifest_env.uploads['up2'] = FakeUploadFiles({'e2': {}})
    response = _response(_entries(('up1', 'e1'), ('up2', 'e2'), ('up1', 'e3')))

    results = list(utils.yield_manifest_archives(_manifest_input(), response))

    assert [r['entry_id'] for r in results] == ['e1', 'e3', 'e2']


def test_yield_manifest_archives_skips_missing_upload(manifest_env):
    manifest_env.uploads['up2'] = FakeUploadFiles({'e2': {}})
    logger = MagicMock()
    response = _response(_entries(('missing', 'e1'), ('up2', 'e2')))

    results = list(utils.yield_manifest_archives(_manifest_input(), response, logger))

    assert [r['entry_id'] for r in results] == ['e2']
    logger.info.assert_any_call('no upload files found for upload_id: missing')


def test_yield_manifest_archives_skips_filtered_and_empty_archives(manifest_env):
    manifest_env.uploads['up1'] = FakeUploadFiles(
        {'keep': {'k': 1}, 'filtered': {'f': 1}, 'empty': None}
    )
    manifest_env.archive_filter.side_effect = lambda entry: (
        entry['entry_id'] == 'filtered'
    )
    response = _response(
        _entries(('up1', 'keep'), ('up1', 'filtered'), ('up1', 'empty'))
    )

    results = list(utils.yield_manifest_archives(_manifest_input(), response))

    assert [r['entry_id'] for r in results] == ['keep']
    assert _read_manifest(manifest_env) == [{'entry_id': 'keep', 'upload_id': 'up1'}]


def test_yield_manifest_archives_logs_and_continues_on_read_error(manifest_env):
    manifest_env.uploads['up1'] = FakeUploadFiles(
        {'bad': {}, 'good': {}}, failing={'bad'}
    )
    logger = MagicMock()
    response = _response(_entries(('up1', 'bad'), ('up1', 'good')))

    results = list(utils.yield_manifest_archives(_manifest_input(), response, logger))

    assert [r['entry_id'] for r in results] == ['good']
    logger.error.assert_called_once()
    assert logger.error.call_args.kwargs['entry_id'] == 'bad'


def test_yield_manifest_archives_read_error_without_logger(manifest_env):
    manifest_env.uploads['up1'] = FakeUploadFiles({'bad': {}}, failing={'bad'})
    response = _response(_entries(('up1', 'bad')))

    assert list(utils.yield_manifest_archives(_manifest_input(), response)) == []
    assert _read_manifest(manifest_env) == []


def test_yield_manifest_archives_stops_at_user_limit(manifest_env):
    manifest_env.uploads['up1'] = FakeUploadFiles({f'e{i}': {} for i in range(5)})
    response = _response(_entries(*[('up1', f'e{i}') for i in range(5)]))

    results = list(utils.yield_manifest_archives(_manifest_input(limit=2), response))

    assert len(results) == 2  # noqa: PLR2004
    assert len(_read_manifest(manifest_env)) == 2  # noqa: PLR2004
    manifest_env.search.assert_not_called()


def test_yield_manifest_archives_respects_config_limit(manifest_env):
    manifest_env.uploads['up1'] = FakeUploadFiles({f'e{i}': {} for i in range(5)})
    response = _response(_entries(*[('up1', f'e{i}') for i in range(5)]))

    with patch.object(utils.config, 'max_entries_export_limit', 3):
        results = list(
            utils.yield_manifest_archives(_manifest_input(limit=100), response)
        )

    assert len(results) == 3  # noqa: PLR2004
    assert len(_read_manifest(manifest_env)) == 3  # noqa: PLR2004


def test_yield_manifest_archives_paginates_until_exhausted(manifest_env):
    manifest_env.uploads['up1'] = FakeUploadFiles({'e1': {}, 'e2': {}, 'e3': {}})
    first = _response(
        _entries(('up1', 'e1'), ('up1', 'e2')), next_page_after_value='c1'
    )
    second = _response(_entries(('up1', 'e3')), next_page_after_value=None)
    manifest_env.search.return_value = second
    data = _manifest_input(limit=50, query={'results.material.elements': ['Si']})

    results = list(utils.yield_manifest_archives(data, first))

    assert [r['entry_id'] for r in results] == ['e1', 'e2', 'e3']
    manifest_env.search.assert_called_once()
    kwargs = manifest_env.search.call_args.kwargs
    assert kwargs['user_id'] == 'user-1'
    assert kwargs['owner'] == 'public'
    assert kwargs['query'] == {'results.material.elements': ['Si']}
    assert kwargs['pagination'].page_size == 50  # noqa: PLR2004
    assert kwargs['pagination'].page_after_value == 'c1'
    assert len(_read_manifest(manifest_env)) == 3  # noqa: PLR2004


def test_yield_manifest_archives_no_new_search_once_limit_reached(manifest_env):
    manifest_env.uploads['up1'] = FakeUploadFiles({'e1': {}, 'e2': {}})
    response = _response(_entries(('up1', 'e1'), ('up1', 'e2')), 'more')

    results = list(utils.yield_manifest_archives(_manifest_input(limit=2), response))

    assert len(results) == 2  # noqa: PLR2004
    manifest_env.search.assert_not_called()


def test_yield_manifest_archives_empty_response_writes_empty_manifest(manifest_env):
    results = list(utils.yield_manifest_archives(_manifest_input(), _response([])))

    assert results == []
    assert _read_manifest(manifest_env) == []
