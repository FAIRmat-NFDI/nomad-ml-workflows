from nomad_ml_workflows.actions.export_entries.models import ReadArchivesInput, Required


def test_build_archive_required_returns_wildcard_for_empty_paths():
    assert ReadArchivesInput.build_archive_required(Required()) == '*'


def test_build_archive_required_builds_nested_include_tree():
    required = Required(include=['data.results.energy'])

    assert ReadArchivesInput.build_archive_required(required) == {
        'data': {'results': {'energy': 'include'}}
    }


def test_build_archive_required_prefers_parent_include():
    required = Required(include=['data', 'data.results'])

    assert ReadArchivesInput.build_archive_required(required) == {'data': 'include'}


def test_build_archive_required_strips_include_wildcard_suffix():
    required = Required(include=['data.results*'])

    assert ReadArchivesInput.build_archive_required(required) == {
        'data': {'results': 'include'}
    }


def test_build_archive_required_builds_include_resolved_directive():
    required = Required(include_resolved=['data.results.energy'])

    assert ReadArchivesInput.build_archive_required(required) == {
        'data': {'results': {'energy': 'include-resolved'}}
    }


def test_build_archive_required_prefers_include_resolved_for_same_path():
    required = Required(
        include=['data.results.energy'],
        include_resolved=['data.results.energy'],
    )

    assert ReadArchivesInput.build_archive_required(required) == {
        'data': {'results': {'energy': 'include-resolved'}}
    }
