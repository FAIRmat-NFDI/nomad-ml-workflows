import json
from datetime import datetime

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from nomad.datamodel.data import EntryData
from nomad.metainfo import MEnum, MSection, Quantity, SubSection
from nomad.metainfo.data_type import JSON, Any, Bytes, NonPrimitive

from nomad_ml_workflows.actions.export_entries import utils


class CustomPayload(NonPrimitive):
    def _normalize_impl(self, value, **kwargs):
        return value


class TypedChild(MSection):
    value = Quantity(type=str)


class TypedEntryData(EntryData):
    count = Quantity(type=np.int32)
    ratio = Quantity(type=np.float32)
    enabled = Quantity(type=bool)
    created = Quantity(type=datetime)
    phase = Quantity(type=MEnum('alpha', 'beta'))
    float_values = Quantity(type=np.float32, shape=['*'])
    str_values = Quantity(type=str, shape=['*'])
    str_matrix = Quantity(type=str, shape=['*', '*'])
    payload = Quantity(type=JSON)
    anything = Quantity(type=Any)
    token = Quantity(type=Bytes)
    complex_value = Quantity(type=np.complex128)
    reference = Quantity(type=TypedChild)
    custom = Quantity(type=CustomPayload)
    children = SubSection(sub_section=TypedChild.m_def, repeats=True)


def _archive(entry_id: str, data: dict) -> dict:
    return {
        'entry_id': entry_id,
        'archive': {
            'data': {
                'm_def': 'test.TypedEntryData',
                **data,
            }
        },
    }


def _column(name: str, section=TypedEntryData) -> str:
    return f'data.{name}#{section.m_def.qualified_name()}'


def test_ordered_columns_places_identifiers_before_sorted_columns():
    assert utils._ordered_columns(
        ['z_column', 'upload_id', 'a_column', 'entry_id']
    ) == ['entry_id', 'upload_id', 'a_column', 'z_column']
    assert utils._ordered_columns(['z_column', 'entry_id', 'a_column']) == [
        'entry_id',
        'a_column',
        'z_column',
    ]
    assert utils._ordered_columns(['z_column', 'entry_id', 'a_column']) == [
        'entry_id',
        'a_column',
        'z_column',
    ]
    assert utils._ordered_columns(
        {'z_column': 'm_def_1', 'entry_id': 'str', 'a_column': 'm_def_2'}
    ) == [
        'entry_id',
        'a_column',
        'z_column',
    ]


@pytest.fixture
def resolve_test_schema(monkeypatch):
    monkeypatch.setattr(
        utils,
        '_resolve_section_def',
        {'test.TypedEntryData': TypedEntryData.m_def}.get,
    )


@pytest.mark.parametrize(
    ('leaf_type', 'is_string'),
    [
        pytest.param(pa.int64(), False, id='int64'),
        pytest.param(pa.float64(), False, id='float64'),
        pytest.param(pa.bool_(), False, id='bool'),
        pytest.param(pa.string(), True, id='string'),
        pytest.param(pa.timestamp('us', tz='UTC'), False, id='datetime'),
    ],
)
@pytest.mark.parametrize('list_depth', [0, 1, 2], ids=['scalar', 'list', 'nested'])
def test_is_list_of_string_for_supported_arrow_types(
    leaf_type,
    is_string,
    list_depth,
):
    arrow_type = leaf_type
    for _ in range(list_depth):
        arrow_type = pa.list_(arrow_type)

    assert utils._is_list_of_string(arrow_type) is (is_string and list_depth > 0)


def test_archives_to_arrow_table_uses_nomad_quantity_types(resolve_test_schema):
    table = utils.archives_to_arrow_table(
        [
            _archive(
                'entry_1',
                {
                    'count': None,
                    'ratio': None,
                    'enabled': None,
                    'created': None,
                    'phase': None,
                    'float_values': None,
                    'str_values': None,
                    'str_matrix': None,
                    'payload': None,
                    'anything': None,
                    'token': None,
                    'complex_value': None,
                    'reference': None,
                    'custom': None,
                },
            ),
            _archive(
                'entry_2',
                {
                    'count': '7',
                    'ratio': '1.5',
                    'enabled': 'true',
                    'created': '2024-01-02T03:04:05Z',
                    'phase': 'alpha',
                    'float_values': ['1.5', 2],
                    'str_values': ['one', 2],
                    'str_matrix': [['one', 'two']],
                    'payload': {'z': 2, 'a': 1},
                    'anything': [1, 'two'],
                    'token': 'YWJj',
                    'complex_value': {'re': 1.0, 'im': 2.0},
                    'reference': '#/data/children/0',
                    'custom': {'answer': 42},
                    'children': [{'value': 'child'}],
                },
            ),
        ]
    )

    assert table.schema.field(_column('count')).type == pa.int32()
    assert table.schema.field(_column('ratio')).type == pa.float32()
    assert table.schema.field(_column('enabled')).type == pa.bool_()
    assert table.schema.field(_column('created')).type == pa.timestamp('us', tz='UTC')
    assert table.schema.field(_column('phase')).type == pa.string()
    assert table.schema.field(_column('float_values')).type == pa.list_(pa.float32())
    assert table.schema.field(_column('str_values')).type == pa.list_(pa.string())
    assert table.schema.field(_column('str_matrix')).type == pa.list_(
        pa.list_(pa.string())
    )
    assert table[_column('count')].to_pylist() == [None, 7]
    assert table[_column('ratio')].to_pylist() == [None, 1.5]
    assert table[_column('enabled')].to_pylist() == [None, True]
    assert table[_column('float_values')].to_pylist() == [None, [1.5, 2.0]]
    assert table[_column('str_values')].to_pylist() == [None, ['one', '2']]
    assert table[_column('str_matrix')].to_pylist() == [None, [['one', 'two']]]
    assert json.loads(table[_column('payload')][1].as_py()) == {'a': 1, 'z': 2}
    assert json.loads(table[_column('anything')][1].as_py()) == [1, 'two']
    assert table[_column('token')].to_pylist() == [None, 'YWJj']
    assert json.loads(table[_column('complex_value')][1].as_py()) == {
        'im': 2.0,
        're': 1.0,
    }
    assert json.loads(table[_column('reference')][1].as_py()) == ('#/data/children/0')
    assert json.loads(table[_column('custom')][1].as_py()) == {'answer': 42}
    assert table[_column('children.0.value', TypedChild)].to_pylist() == [None, 'child']


@pytest.mark.parametrize(
    ('quantity_name', 'value'),
    [
        ('float_values', 2),
        ('str_values', 'two'),
        ('str_matrix', 'two'),
        ('str_matrix', ['two']),
        ('str_matrix', ['one', ['two']]),
    ],
)
def test_archives_to_arrow_table_handles_invalid_list_shapes_with_None(
    resolve_test_schema,
    quantity_name,
    value,
):
    table = utils.archives_to_arrow_table([_archive('entry_1', {quantity_name: value})])
    assert table.column_names == ['entry_id', _column(quantity_name)]
    assert table[_column(quantity_name)].to_pylist() == [None]


def test_archives_to_arrow_table_handles_failed_conversion_with_None(
    resolve_test_schema,
):
    table = utils.archives_to_arrow_table(
        [_archive('entry_1', {'count': 'not-an-integer'})]
    )
    assert table[_column('count')].to_pylist() == [None]


def test_archives_to_dataframe_preserves_existing_behavior(resolve_test_schema):
    dataframe = utils.archives_to_dataframe([_archive('entry_1', {'count': '7'})])

    assert dataframe[_column('count')].iloc[0] == '7'


def test_write_parquet_file_uses_schema_normalized_table(tmp_path, resolve_test_schema):
    output_path = tmp_path / 'entries.parquet'

    utils.write_parquet_file(
        str(output_path),
        [_archive('entry_1', {'count': None}), _archive('entry_2', {'count': '7'})],
    )

    table = pq.read_table(output_path)
    assert table.schema.field(_column('count')).type == pa.int32()
    assert table[_column('count')].to_pylist() == [None, 7]
