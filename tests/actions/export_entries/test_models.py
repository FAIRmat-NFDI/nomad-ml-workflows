from nomad_ml_workflows.actions.export_entries.models import (
    Exclude,
    ExportEntriesUserInput,
    Include,
    NormalizedSearchSettings,
)


def test_user_input():
    entry_limit = 25
    user_input = ExportEntriesUserInput.model_validate(
        {
            'user_id': 'user-id',
            'upload_id': 'project-id',
            'search_settings': {
                'owner': 'public',
                'max_entries': entry_limit,
                'query': '{\n  "entry_type": "ELNSample"\n}',
                'required': [
                    {
                        'type': 'include',
                        'path': 'results',
                        'resolve_references': False,
                    }
                ],
            },
            'export_settings': {
                'file_format': 'csv',
                'create_zip_archive': False,
            },
        }
    )

    assert user_input.search_settings.owner == 'public'
    assert user_input.search_settings.max_entries == entry_limit
    assert user_input.search_settings.required == [
        Include(type='include', path='results', resolve_references=False)
    ]
    assert user_input.export_settings.file_format == 'csv'
    assert user_input.export_settings.create_zip_archive is False

    normalized = NormalizedSearchSettings.from_user_input(user_input)
    assert normalized.owner == 'public'
    assert normalized.query == {'entry_type': 'ELNSample'}
    assert normalized.num_entries_user_limit == entry_limit
    assert normalized.required == {'results': 'include'}


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
