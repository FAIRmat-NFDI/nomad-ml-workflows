from nomad_ml_workflows.actions.export_entries.models import (
    Exclude,
    Include,
    NormalizedSearchSettings,
)


def test_build_archive_required_returns_wildcard_for_empty_paths():
    assert NormalizedSearchSettings.build_archive_required(None) == '*'


def test_build_archive_required_builds_nested_include_tree():
    required = [Include(path='data.results.energy', resolve_references=False)]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        'data': {'results': {'energy': 'include'}}
    }


def test_build_archive_required_prefers_parent_include():
    required = [
        Include(path='data', resolve_references=False),
        Include(path='data.results', resolve_references=False),
    ]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        'data': 'include'
    }


def test_build_archive_required_strips_include_wildcard_suffix():
    required = [Include(path='data.results*', resolve_references=False)]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        'data': {'results': 'include'}
    }


def test_build_archive_required_builds_include_resolved_directive():
    required = [Include(path='data.results.energy', resolve_references=True)]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        'data': {'results': {'energy': 'include-resolved'}}
    }


def test_build_archive_required_prefers_include_resolved_for_same_path():
    required = [
        Include(path='data.results.energy', resolve_references=False),
        Include(path='data.results.energy', resolve_references=True),
    ]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        'data': {'results': {'energy': 'include-resolved'}}
    }


def test_build_archive_required_exclude():
    required = [Exclude(path='data.results.energy')]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        '*': 'include',
        'data': {'results': {'energy': 'exclude'}},
    }


def test_build_archive_required_exclude_over_include():
    required = [
        Include(path='data.results.energy', resolve_references=False),
        Exclude(path='data.results.energy'),
    ]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        '*': 'include',
        'data': {'results': {'energy': 'exclude'}},
    }


def test_build_archive_required_include_nested_exclude():
    required = [
        Include(path='data', resolve_references=False),
        Exclude(path='data.results.energy'),
    ]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        'data': {
            '*': 'include',
            'results': {'energy': 'exclude'},
        }
    }


def test_build_archive_required_include_resolved_nested_exclude():
    required = [
        Include(path='data', resolve_references=True),
        Exclude(path='data.results.energy'),
    ]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        'data': {
            '*': 'include-resolved',
            'results': {'energy': 'exclude'},
        }
    }


def test_build_archive_required_same_path_multiple_times():
    required = [
        Include(path='data.results.energy', resolve_references=True),
        Include(path='data.results.energy', resolve_references=False),
        Exclude(path='data.results.energy'),
    ]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        '*': 'include',
        'data': {'results': {'energy': 'exclude'}},
    }


def test_build_archive_required_prefers_include_resolved_for_whole_parent():
    """
    TODO: Remove once wildcard '*' as a key is supported in RequiredReader.

    Ideally, this should give: {
        'data': {
            '*': 'include',
            'results': {'energy': 'include-resolved'},
        }
    }
    """
    required = [
        Include(path='data', resolve_references=False),
        Include(path='data.results.energy', resolve_references=True),
    ]

    assert NormalizedSearchSettings.build_archive_required(required) == {
        'data': 'include-resolved'
    }
