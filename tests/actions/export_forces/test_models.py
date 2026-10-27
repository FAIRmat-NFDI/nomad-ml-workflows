import json

import pytest
from pydantic import ValidationError

from nomad_ml_workflows.actions.export_forces.models import (
    DataFileFormat,
    ForcesExportEntriesUserInput,
    ForcesExportSettings,
    ForcesNormalizedSearchSettings,
    ForcesSearchSettings,
    IncludeProperties,
    _clean_field,
    config,
)


def _user_input(**search_overrides) -> ForcesExportEntriesUserInput:
    search_settings = {
        'owner': 'public',
        'max_entries': 25,
        'query': '{"results.method.method_name": "DFT"}',
        'properties': {'forces': True},
        **search_overrides,
    }
    return ForcesExportEntriesUserInput.model_validate(
        {
            'user_id': 'user-id',
            'upload_id': 'upload-id',
            'search_settings': search_settings,
            'export_settings': {'file_format': {'extxyz': True}},
        }
    )


@pytest.mark.parametrize(
    'raw, expected',
    [
        ('  {"a": 1}  ', '{"a": 1}'),
        ('\'{"a": 1}\'', '{"a": 1}'),
        ('"{\\"a\\": 1}"', '{\\"a\\": 1}'),
        ('plain', 'plain'),
    ],
)
def test_clean_field(raw, expected):
    assert _clean_field(raw) == expected


def test_search_settings_defaults():
    settings = ForcesSearchSettings(properties=IncludeProperties(energies=True))

    assert settings.owner == 'visible'
    assert settings.max_entries == min(1000, config.max_entries_export_limit)
    assert settings.max_frames == config.max_frames_export_limit
    assert isinstance(json.loads(settings.query), dict)


def test_search_settings_requires_properties():
    with pytest.raises(ValidationError, match='properties'):
        ForcesSearchSettings()  # type: ignore[call-arg]


def test_search_settings_rejects_no_property_selected():
    with pytest.raises(ValidationError, match='At least one of Energies'):
        ForcesSearchSettings(properties=IncludeProperties())


@pytest.mark.parametrize('max_entries', [0, -1, config.max_entries_export_limit + 1])
def test_search_settings_max_entries_out_of_range(max_entries):
    with pytest.raises(ValidationError, match='max_entries'):
        ForcesSearchSettings(
            max_entries=max_entries, properties=IncludeProperties(forces=True)
        )


@pytest.mark.parametrize('max_frames', [0, config.max_frames_export_limit + 1])
def test_search_settings_max_frames_out_of_range(max_frames):
    with pytest.raises(ValidationError, match='max_frames'):
        ForcesSearchSettings(
            max_frames=max_frames, properties=IncludeProperties(forces=True)
        )


def test_search_settings_rejects_unknown_owner():
    with pytest.raises(ValidationError, match='owner'):
        ForcesSearchSettings(
            owner='everyone',  # type: ignore[arg-type]
            properties=IncludeProperties(forces=True),
        )


@pytest.mark.parametrize(
    'flags, expected',
    [
        ({'energies': True}, ['energy']),
        ({'forces': True}, ['forces']),
        ({'stresses': True}, ['stress']),
        (
            {'energies': True, 'forces': True, 'stresses': True},
            ['energy', 'forces', 'stress'],
        ),
        ({'stresses': True, 'energies': True}, ['energy', 'stress']),
    ],
)
def test_required_properties(flags, expected):
    settings = ForcesSearchSettings(properties=IncludeProperties(**flags))

    assert settings.required_properties == expected


@pytest.mark.parametrize(
    'flags, expected',
    [
        ({}, []),
        ({'extxyz': True}, ['extxyz']),
        ({'ase_db': True}, ['ase_db']),
        ({'extxyz': True, 'ase_db': True}, ['extxyz', 'ase_db']),
    ],
)
def test_selected_formats(flags, expected):
    assert DataFileFormat(**flags).selected_formats == expected


def test_export_settings_defaults_to_zip_archive():
    settings = ForcesExportSettings(file_format=DataFileFormat(ase_db=True))

    assert settings.create_zip_archive is True


def test_export_settings_rejects_no_file_format_selected():
    with pytest.raises(ValidationError, match='At least one of extxyz or ase_db'):
        ForcesExportSettings(file_format=DataFileFormat())


def test_user_input_parses_nested_settings():
    user_input = _user_input()

    assert user_input.user_id == 'user-id'
    assert user_input.upload_id == 'upload-id'
    assert user_input.search_settings.owner == 'public'
    assert user_input.search_settings.max_entries == 25  # noqa: PLR2004
    assert user_input.search_settings.required_properties == ['forces']
    assert user_input.export_settings.file_format.selected_formats == ['extxyz']


def test_normalized_search_settings_from_user_input():
    normalized = ForcesNormalizedSearchSettings.from_user_input(
        _user_input(properties={'energies': True, 'stresses': True})
    )

    assert normalized.user_id == 'user-id'
    assert normalized.owner == 'public'
    assert normalized.num_entries_user_limit == 25  # noqa: PLR2004
    assert normalized.query['results.method.method_name'] == 'DFT'  # type: ignore[index]
    assert normalized.query['quantities:all'] == [  # type: ignore[index]
        'run.calculation.energy.total.value',
        'run.calculation.stress.total.value',
    ]


def test_normalized_search_settings_extends_existing_quantities():
    normalized = ForcesNormalizedSearchSettings.from_user_input(
        _user_input(query='{"quantities:all": ["run.system"]}')
    )

    assert normalized.query['quantities:all'] == [  # type: ignore[index]
        'run.system',
        'run.calculation.forces.total.value',
    ]


def test_normalized_search_settings_accepts_single_quoted_query():
    normalized = ForcesNormalizedSearchSettings.from_user_input(
        _user_input(query="{'results.material.elements': ['Si']}")
    )

    assert normalized.query['results.material.elements'] == ['Si']  # type: ignore[index]


def test_normalized_search_settings_strips_wrapping_quotes_and_whitespace():
    normalized = ForcesNormalizedSearchSettings.from_user_input(
        _user_input(query='  \'{"results.material.elements": ["Si"]}\'  ')
    )

    assert normalized.query['results.material.elements'] == ['Si']  # type: ignore[index]


def test_normalized_search_settings_invalid_json_query():
    with pytest.raises(json.JSONDecodeError):
        ForcesNormalizedSearchSettings.from_user_input(_user_input(query='not json'))
