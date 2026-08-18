import csv
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


def _write_test_schema(tmp_path, quantity_defs: dict[str, Quantity]):
    schema_path = tmp_path / 'table_schema.arrow'
    column_configs = {}
    for path, quantity in quantity_defs.items():
        column_configs[path] = utils._quantity_to_arrow_column_config(quantity)
    utils._write_table_schema(
        schema_path,
        column_configs,
    )
    return schema_path


def _archives_to_arrow_table(archives: list[dict]) -> pa.Table:
    table_rows = [
        utils._flatten_entry_archive(
            archive['archive'],
            entry_id=archive['entry_id'],
            upload_id=archive.get('upload_id', ''),
        )
        for archive in archives
    ]
    columns_quantity_def = {
        column: quantity_def
        for table_row in table_rows
        for column, quantity_def in table_row.columns_quantity_def.items()
    }
    column_configs = {}
    for path, quantity in columns_quantity_def.items():
        column_configs[path] = utils._quantity_to_arrow_column_config(quantity)
    column_configs = utils._ensure_column_configs_consistency(column_configs)
    schema = pa.schema(
        [
            pa.field(column_name, config.arrow_type)
            for column_name, config in column_configs.items()
        ]
    )
    batch = utils._table_rows_to_arrow_batch(
        [table_row.data_dict for table_row in table_rows],
        column_configs,
        schema,
    )
    return pa.Table.from_batches([batch], schema=schema)


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


def test_table_schema_round_trip_preserves_order_types_and_normalization(tmp_path):
    schema_path = tmp_path / 'table_schema.arrow'
    expected_configs = {}

    for path, quantity in {
        'values': Quantity(type=str, shape=['*']),
        'payload': Quantity(type=JSON),
        'created': Quantity(type=datetime),
    }.items():
        expected_configs[path] = utils._quantity_to_arrow_column_config(quantity)

    utils._write_table_schema(schema_path, expected_configs)
    actual_configs, arrow_schema = utils._read_table_schema(schema_path)

    assert list(actual_configs) == [
        'entry_id',
        'upload_id',
        'created',
        'payload',
        'values',
    ]
    assert actual_configs == expected_configs
    assert arrow_schema.field('created').type == pa.timestamp('us', tz='UTC')
    assert arrow_schema.field('values').type == pa.list_(pa.string())
    assert actual_configs['payload'].stringify_json is True
    assert arrow_schema.metadata is None
    assert all(field.metadata is None for field in arrow_schema)


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


def test_table_rows_to_arrow_batch_uses_nomad_quantity_types(resolve_test_schema):
    table = _archives_to_arrow_table(
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
def test_table_rows_to_arrow_batch_handles_invalid_list_shapes_with_none(
    resolve_test_schema,
    quantity_name,
    value,
):
    table = _archives_to_arrow_table([_archive('entry_1', {quantity_name: value})])
    assert table.column_names == ['entry_id', 'upload_id', _column(quantity_name)]
    assert table[_column(quantity_name)].to_pylist() == [None]


def test_table_rows_to_arrow_batch_handles_failed_conversion_with_none(
    resolve_test_schema,
):
    table = _archives_to_arrow_table([_archive('entry_1', {'count': 'not-an-integer'})])
    assert table[_column('count')].to_pylist() == [None]


def test_write_table_rows_to_tabular_file_uses_normalized_schema(
    tmp_path,
    resolve_test_schema,
    monkeypatch,
):
    rows_path = tmp_path / 'entries.ndjson'
    schema_path = tmp_path / 'entries_schema.arrow'
    output_path = tmp_path / 'entries.parquet'
    table_rows = [
        utils._flatten_entry_archive(
            archive['archive'],
            entry_id=archive['entry_id'],
        )
        for archive in [
            _archive('entry_1', {'count': None}),
            _archive('entry_2', {'count': '7'}),
        ]
    ]
    monkeypatch.setattr(utils, 'generate_table_rows', lambda *_: iter(table_rows))
    utils.write_table_rows_to_ndjson([], {}, 'user_id', rows_path, schema_path)

    count = utils.write_table_rows_to_tabular_file(
        rows_path,
        output_path,
        schema_path,
    )

    table = pq.read_table(output_path)
    assert count == len(table_rows)
    assert table.schema.field(_column('count')).type == pa.int32()
    assert table[_column('count')].to_pylist() == [None, 7]


def test_write_table_rows_to_ndjson_accepts_repeated_quantity_definition(
    tmp_path, monkeypatch
):
    output_path = tmp_path / 'rows.ndjson'
    schema_path = tmp_path / 'table_schema.arrow'
    quantity_def = Quantity(type=str)
    table_rows = [
        utils.FlatEntryArchive(
            data_dict={'entry_id': 'one'},
            columns_quantity_def={'value': quantity_def},
            unhandled_keys=[],
        ),
        utils.FlatEntryArchive(
            data_dict={'entry_id': 'two'},
            columns_quantity_def={'value': quantity_def},
            unhandled_keys=[],
        ),
    ]
    monkeypatch.setattr(utils, 'generate_table_rows', lambda *_: iter(table_rows))

    result = utils.write_table_rows_to_ndjson(
        [], {}, 'user_id', output_path, schema_path
    )

    assert result is None
    assert output_path.read_text(encoding='utf-8') == (
        '{"entry_id":"one"}\n{"entry_id":"two"}\n'
    )
    column_configs, arrow_schema = utils._read_table_schema(schema_path)
    assert list(column_configs) == ['entry_id', 'upload_id', 'value']
    assert arrow_schema.field('value').type == pa.string()


def test_write_table_rows_to_tabular_file_reads_ndjson(tmp_path):
    rows_path = tmp_path / 'rows.ndjson'
    output_path = tmp_path / 'rows.parquet'
    quantity_def = Quantity(type=str)
    rows_path.write_text(
        '{"entry_id":"one","upload_id":"upload","value":"first"}\n'
        '{"entry_id":"two","upload_id":"upload","value":"second"}\n',
        encoding='utf-8',
    )

    count = utils.write_table_rows_to_tabular_file(
        rows_path,
        output_path,
        _write_test_schema(tmp_path, {'value': quantity_def}),
    )

    table = pq.read_table(output_path)
    assert count == table.num_rows
    assert table.column_names == ['entry_id', 'upload_id', 'value']
    assert table['entry_id'].to_pylist() == ['one', 'two']
    assert table['value'].to_pylist() == ['first', 'second']


def test_write_table_rows_to_tabular_file_builds_wide_multi_row_batches(
    tmp_path,
    monkeypatch,
):
    rows_path = tmp_path / 'rows.ndjson'
    output_path = tmp_path / 'rows.parquet'
    quantity_defs = {f'value_{index:03}': Quantity(type=str) for index in range(64)}
    rows = [
        {
            'entry_id': f'entry_{index}',
            'upload_id': 'upload',
            f'value_{index:03}': f'content_{index}',
        }
        for index in range(5)
    ]
    rows_path.write_text(
        ''.join(f'{json.dumps(row, separators=(",", ":"))}\n' for row in rows),
        encoding='utf-8',
    )
    batch_sizes = []
    original_converter = utils._table_rows_to_arrow_batch

    def capture_batch_size(batch_rows, *args, **kwargs):
        batch_sizes.append(len(batch_rows))
        return original_converter(batch_rows, *args, **kwargs)

    monkeypatch.setattr(utils, '_table_rows_to_arrow_batch', capture_batch_size)
    count = utils.write_table_rows_to_tabular_file(
        rows_path,
        output_path,
        _write_test_schema(tmp_path, quantity_defs),
        max_buffer_bytes=1024 * 1024,
        max_buffer_rows=3,
    )

    table = pq.read_table(output_path)
    parquet_file = pq.ParquetFile(output_path)
    expected_batch_sizes = [3, 2]
    assert count == len(rows)
    assert batch_sizes == expected_batch_sizes
    assert parquet_file.metadata.num_row_groups == len(expected_batch_sizes)
    assert table['entry_id'].to_pylist() == [row['entry_id'] for row in rows]


def test_write_table_rows_to_tabular_file_flushes_on_input_bytes(
    tmp_path,
    monkeypatch,
):
    rows_path = tmp_path / 'rows.ndjson'
    output_path = tmp_path / 'rows.parquet'
    rows = [
        {'entry_id': f'entry_{index}', 'upload_id': 'upload', 'value': 'content'}
        for index in range(3)
    ]
    encoded_lines = [
        f'{json.dumps(row, separators=(",", ":"))}\n'.encode() for row in rows
    ]
    rows_path.write_bytes(b''.join(encoded_lines))
    batch_sizes = []
    original_converter = utils._table_rows_to_arrow_batch

    def capture_batch_size(batch_rows, *args, **kwargs):
        batch_sizes.append(len(batch_rows))
        return original_converter(batch_rows, *args, **kwargs)

    monkeypatch.setattr(utils, '_table_rows_to_arrow_batch', capture_batch_size)

    count = utils.write_table_rows_to_tabular_file(
        rows_path,
        output_path,
        _write_test_schema(tmp_path, {'value': Quantity(type=str)}),
        max_buffer_bytes=len(encoded_lines[0]) + len(encoded_lines[1]),
    )

    expected_batch_sizes = [2, 1]
    assert count == len(rows)
    assert batch_sizes == expected_batch_sizes
    assert pq.ParquetFile(output_path).metadata.num_row_groups == len(
        expected_batch_sizes
    )


def test_write_table_rows_to_tabular_file_writes_oversized_row_immediately(
    tmp_path,
    monkeypatch,
):
    rows_path = tmp_path / 'rows.ndjson'
    output_path = tmp_path / 'rows.parquet'
    rows = [
        {'entry_id': 'large', 'upload_id': 'upload', 'value': 'x' * 1000},
        {'entry_id': 'small', 'upload_id': 'upload', 'value': 'x'},
    ]
    encoded_lines = [
        f'{json.dumps(row, separators=(",", ":"))}\n'.encode() for row in rows
    ]
    rows_path.write_bytes(b''.join(encoded_lines))
    batch_sizes = []
    warnings = []
    original_converter = utils._table_rows_to_arrow_batch

    def capture_batch_size(batch_rows, *args, **kwargs):
        batch_sizes.append(len(batch_rows))
        return original_converter(batch_rows, *args, **kwargs)

    class CapturingLogger:
        def info(self, message):
            pass

        def warning(self, message, **kwargs):
            warnings.append((message, kwargs))

    monkeypatch.setattr(utils, '_table_rows_to_arrow_batch', capture_batch_size)

    count = utils.write_table_rows_to_tabular_file(
        rows_path,
        output_path,
        _write_test_schema(tmp_path, {'value': Quantity(type=str)}),
        max_buffer_bytes=len(encoded_lines[1]) + 1,
        logger=CapturingLogger(),
    )

    assert count == len(rows)
    assert batch_sizes == [1, 1]
    assert len(warnings) == 1
    assert warnings[0][1]['entry_id'] == 'large'
    assert warnings[0][1]['row_input_bytes'] == len(encoded_lines[0])


def test_write_table_rows_to_tabular_file_stringifies_nested_csv_values(tmp_path):
    rows_path = tmp_path / 'rows.ndjson'
    output_path = tmp_path / 'rows.csv'
    values = [['one', 'two'], None, ['three']]
    rows = [
        {
            'entry_id': f'entry_{index}',
            'upload_id': 'upload',
            'values': value,
        }
        for index, value in enumerate(values)
    ]
    rows_path.write_text(
        ''.join(f'{json.dumps(row, separators=(",", ":"))}\n' for row in rows),
        encoding='utf-8',
    )
    count = utils.write_table_rows_to_tabular_file(
        rows_path,
        output_path,
        _write_test_schema(
            tmp_path,
            {'values': Quantity(type=str, shape=['*'])},
        ),
        max_buffer_rows=2,
    )

    with output_path.open(newline='', encoding='utf-8') as output_file:
        output_rows = list(csv.DictReader(output_file))
    assert count == len(rows)
    assert [row['entry_id'] for row in output_rows] == [row['entry_id'] for row in rows]
    assert [
        json.loads(row['values']) if row['values'] else None for row in output_rows
    ] == values
