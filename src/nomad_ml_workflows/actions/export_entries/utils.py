from __future__ import annotations

import importlib
import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any

from nomad.app.v1.models.models import User
from nomad.archive.required import RequiredReader
from nomad.datamodel.datamodel import EntryArchive, EntryData
from nomad.files import UploadFiles
from nomad.metainfo import data_type as nomad_data_type
from nomad.metainfo.metainfo import Quantity, Reference, Section

from nomad_ml_workflows.actions.export_entries.models import ManifestEntry

if TYPE_CHECKING:
    from pathlib import Path

    import pyarrow as pa


IGNORED_KEYS = ['m_def', 'm_def_id', 'm_ref_archives']
_STRINGIFY_JSON_KEY = b'nomad:stringify-json'


@lru_cache(maxsize=1)
def require_pyarrow() -> tuple[Any, Any, Any]:
    """Load the optional PyArrow modules when tabular export needs them."""
    try:
        import pyarrow as pa
        import pyarrow.csv as pcsv
        import pyarrow.parquet as pq
    except ImportError as e:
        raise ImportError(
            'pyarrow is required. Install with: '
            'pip install nomad-ml-workflows[cpu-action]'
        ) from e
    return pa, pcsv, pq


@dataclass(frozen=True)
class _ArrowColumnConfig:
    arrow_type: pa.DataType
    stringify_json: bool = False


@dataclass
class FlatEntryArchive:
    data_dict: dict[str, str | int | float | bool | dict | list | None]
    columns_quantity_def: dict[str, Quantity]
    unhandled_keys: list[str]


def _join_path(prefix: str, key: str) -> str:
    return f'{prefix}.{key}' if prefix else key


@lru_cache(maxsize=256)
def _resolve_section_def(m_def: str | None) -> Section | None:
    """
    Get a section def (Section.m_def) for given `m_def` string by importing the
    associated section class.

    TODO: Add support for resolving sections defined using YAML schema.
    """
    if not m_def:
        return None

    if '.' not in m_def:
        return None

    package_name, section_name = m_def.rsplit('.', 1)
    try:
        module = importlib.import_module(package_name)
    except (ImportError, TypeError):
        # skip section resolution when module is not found
        # or when the module is not a valid Python module
        # which happens entry uses a YAML schema
        module = None

    if module is not None:
        section = getattr(module, section_name, None)
        section_def = getattr(section, 'm_def', None)
        if isinstance(section_def, Section):
            return section_def


def _quantity_to_arrow_column_config(quantity_def: Quantity) -> _ArrowColumnConfig:
    """
    Create an Arrow column config from a NOMAD quantity definition. Based on the quantity's type,
    assigns the appropriate Arrow data type and stringify_json flag. If the quantity's shape is
    available, integrates it into the Arrow data type.
    """
    pa, _, _ = require_pyarrow()
    quantity_type = quantity_def.type

    if isinstance(quantity_type, Reference):
        return _ArrowColumnConfig(pa.string(), stringify_json=True)

    if isinstance(quantity_type, nomad_data_type.JSON | nomad_data_type.Any):
        return _ArrowColumnConfig(pa.string(), stringify_json=True)

    try:
        standard_type = quantity_type.standard_type()  # type: ignore
    except (AttributeError, NotImplementedError, TypeError, ValueError):
        return _ArrowColumnConfig(pa.string(), stringify_json=True)

    primitive_types = {
        'int8': pa.int8(),
        'int16': pa.int16(),
        'int32': pa.int32(),
        'int64': pa.int64(),
        'float16': pa.float32(),  # Parquet has no native half-float representation
        'float32': pa.float32(),
        'float64': pa.float64(),
        'bool': pa.bool_(),
        'str': pa.string(),
        'enum': pa.string(),
        'datetime': pa.timestamp('us', tz='UTC'),
        'bytes': pa.string(),  # NOMAD serializes bytes as base64 ASCII text
    }
    arrow_type = primitive_types.get(standard_type)
    if arrow_type is None or standard_type.startswith('complex'):
        return _ArrowColumnConfig(pa.string(), stringify_json=True)

    for _ in quantity_def.shape or []:
        # adds a dimension per `shape: list` element.
        arrow_type = pa.list_(arrow_type)

    return _ArrowColumnConfig(arrow_type)


def _json_stringify(value):
    """Encode value as compact JSON text."""
    if value is None:
        return None
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def _is_list_of_string(arrow_type: pa.DataType) -> bool:
    """Return whether the given Arrow type is a list or nested list of strings."""
    pa, _, _ = require_pyarrow()
    if pa.types.is_list(arrow_type):
        if pa.types.is_string(arrow_type.value_type):
            return True
        if pa.types.is_list(arrow_type.value_type):
            return _is_list_of_string(arrow_type.value_type)
    return False


def _cast_arrow_value(value, arrow_type: pa.DataType):
    """
    Cast a scalar or nested list recursively using Arrow's safe casts.

    Arrow can cast homogeneous lists directly, but cannot infer a source type for
    heterogeneous lists. Recursion lets each list element be cast independently.
    """
    pa, _, _ = require_pyarrow()
    if value is None:
        return None
    if pa.types.is_list(arrow_type):
        if not isinstance(value, list | tuple) and hasattr(value, 'tolist'):
            value = value.tolist()
        if not isinstance(value, list | tuple):
            raise TypeError(f'{value!r} is not a list.')
        return [_cast_arrow_value(item, arrow_type.value_type) for item in value]
    return pa.scalar(value).cast(arrow_type, safe=True).as_py()


def _normalize_arrow_column(
    values: list,
    config: _ArrowColumnConfig,
    column_name: str,
    logger=None,
) -> pa.Array:
    """
    Build a typed Arrow array. It converts mismatched values, when needed, using Arrow's
    safe cast.
    """
    pa, _, _ = require_pyarrow()
    if config.stringify_json:
        try:
            return pa.array(
                [_json_stringify(value) for value in values],
                type=config.arrow_type,
            )
        except (TypeError, ValueError) as e:
            if logger:
                logger.warning(
                    f'Cannot JSON-encode values in column {column_name!r}.',
                    exc_info=e,
                )
            return pa.array([None for _ in values], type=config.arrow_type, safe=True)

    try:
        if _is_list_of_string(config.arrow_type):
            # If a row value is string, it can be silently converted to list of alphabets
            # e.g. "abc" -> ["a", "b", "c"]
            # Each row value is later converted individually to avoid Arrow's safe cast
            # behavior.
            raise AssertionError("Do not use Arrow's safe cast for list of strings")
        # Attempt to bulk convert values to Arrow array using safe cast
        return pa.array(values, type=config.arrow_type, safe=True)
    except (
        pa.ArrowInvalid,
        pa.ArrowNotImplementedError,
        pa.ArrowTypeError,
        OverflowError,
        AssertionError,
    ):
        pass

    converted_values = []
    for row_index, value in enumerate(values):
        if value is None:
            converted_values.append(None)
            continue

        try:
            converted_values.append(_cast_arrow_value(value, config.arrow_type))
        except (
            pa.ArrowInvalid,
            pa.ArrowNotImplementedError,
            pa.ArrowTypeError,
            OverflowError,
            TypeError,
            ValueError,
        ) as e:
            if logger:
                logger.warning(
                    f'Cannot convert row {row_index} of column {column_name!r} '
                    f'to {config.arrow_type}: {value!r}.',
                    exc_info=e,
                )
            converted_values.append(None)

    return pa.array(converted_values, type=config.arrow_type, safe=True)


def _store_leaf_value(row: dict, col: str, value):
    """Store a value as one opaque DataFrame cell."""
    row[col] = value


def _flatten_section(
    section_data: dict,
    section_def: Section,
    prefix: str,
    output: FlatEntryArchive,
):
    """
    Flatten one serialized NOMAD section using its metainfo definition.

    The traversal only recurses through declared subsections. Every quantity is
    treated as an opaque leaf value, regardless of whether it stores a scalar,
    array, nested list, or JSON payload.
    """
    handled_keys = set()

    if prefix == 'data' and section_def == EntryData.m_def:
        m_def = section_data.get('m_def')
        if m_def != section_def.qualified_name():
            raise AssertionError(
                f'archive.data exists but schema definition "{m_def}" not found. Skipping entry.'
            )

    for quantity_name, quantity_def in section_def.all_quantities.items():  # type: ignore
        if quantity_name not in section_data:
            continue
        handled_keys.add(quantity_name)
        col = f'{_join_path(prefix, quantity_name)}#{section_def.qualified_name()}'
        output.columns_quantity_def.setdefault(col, quantity_def)
        _store_leaf_value(output.data_dict, col, section_data[quantity_name])

    for sub_section_name, sub_section_def in section_def.all_sub_sections.items():  # type: ignore
        if sub_section_name not in section_data:
            continue

        handled_keys.add(sub_section_name)
        sub_section_value = section_data[sub_section_name]
        if sub_section_value is None:
            continue

        sub_section_prefix = _join_path(prefix, sub_section_name)
        child_section_def = sub_section_def.sub_section.m_resolved()

        if sub_section_def.repeats:
            for index, item in enumerate(sub_section_value):
                item_prefix = _join_path(sub_section_prefix, str(index))
                # Of m_def is available in the dict, get a section_def based on it.
                # Useful when available data uses a child section of the subsection's
                # original section_def.
                item_section_def = _resolve_section_def(item.get('m_def'))
                _flatten_section(
                    item,
                    item_section_def or child_section_def,
                    item_prefix,
                    output,
                )
        else:
            # If m_def is available in the dict, get a section_def based on it.
            # Useful when available data uses a child section of the subsection's
            # original section_def.
            item_section_def = _resolve_section_def(sub_section_value.get('m_def'))
            _flatten_section(
                sub_section_value,
                item_section_def or child_section_def,
                sub_section_prefix,
                output,
            )

    # Add the unhandled keys to a list. These point to the obsolete data in the
    # archives that do not correspond to a subsection/quantity in the current schema
    for key, _ in section_data.items():
        if key not in [*handled_keys, *IGNORED_KEYS]:
            output.unhandled_keys.append(
                f'{prefix}.{key}#{section_def.qualified_name()}'
            )


def _flatten_entry_archive(
    entry_archive: dict,
    entry_id: str = '',
    upload_id: str = '',
) -> FlatEntryArchive:
    """
    Convert a nested entry archive dict into a flat dict.

    `archive` should corresponds to serialization of EntryArchive::
        "archive": {
            "results": dict,
            "metadata": dict,
            "data": dict,
            "processing_log": list[Any],
            ...
        }
    """
    output = FlatEntryArchive(
        data_dict={
            'entry_id': entry_id,
            'upload_id': upload_id,
        },
        columns_quantity_def={},
        unhandled_keys=[],
    )
    _flatten_section(
        section_data=entry_archive,
        section_def=EntryArchive.m_def,
        prefix='',
        output=output,
    )
    return output


def _ordered_columns(columns: Iterable[str]) -> list[str]:
    """Put entry_id and upload_id first and sort all remaining columns alphabetically."""
    column_set = set(columns)
    identifier_columns = [
        column for column in ('entry_id', 'upload_id') if column in column_set
    ]
    remaining_columns = sorted(column_set.difference(identifier_columns))
    return [*identifier_columns, *remaining_columns]


def _add_ids_and_order_column_configs(
    configs: dict[str, _ArrowColumnConfig],
) -> dict[str, _ArrowColumnConfig]:
    """Add identifier columns and return configs in deterministic column order."""
    pa, _, _ = require_pyarrow()
    configs['entry_id'] = _ArrowColumnConfig(pa.string())
    configs['upload_id'] = _ArrowColumnConfig(pa.string())
    return {column: configs[column] for column in _ordered_columns(configs)}


def _arrow_schema_from_column_configs(
    column_configs: dict[str, _ArrowColumnConfig],
) -> pa.Schema:
    """Build an output Arrow schema with column configs."""
    pa, _, _ = require_pyarrow()
    return pa.schema(
        [
            pa.field(column_name, config.arrow_type)
            for column_name, config in column_configs.items()
        ]
    )


def _write_table_schema(
    schema_file_path: Path,
    column_configs: dict[str, _ArrowColumnConfig],
) -> None:
    """Serialize ordered column configs as an Arrow IPC schema sidecar."""
    pa, _, _ = require_pyarrow()
    stored_schema = pa.schema(
        [
            pa.field(
                column_name,
                config.arrow_type,
                metadata={
                    _STRINGIFY_JSON_KEY: (
                        b'true' if config.stringify_json else b'false'
                    )
                },
            )
            for column_name, config in column_configs.items()
        ]
    )
    with open(schema_file_path, 'wb') as schema_file:
        schema_file.write(stored_schema.serialize())


def _read_table_schema(
    schema_file_path: Path,
) -> tuple[dict[str, _ArrowColumnConfig], pa.Schema]:
    """Load column configs and a metadata-free output schema from a sidecar."""
    pa, _, _ = require_pyarrow()
    with pa.memory_map(str(schema_file_path), 'r') as source:
        stored_schema = pa.ipc.read_schema(source)

    column_configs: dict[str, _ArrowColumnConfig] = {}
    for field in stored_schema:
        stringify_json = (field.metadata or {}).get(_STRINGIFY_JSON_KEY)
        if stringify_json not in {b'true', b'false'}:
            raise ValueError(
                f'Missing or invalid stringify-json setting for column {field.name!r}.'
            )
        column_configs[field.name] = _ArrowColumnConfig(
            arrow_type=field.type,
            stringify_json=stringify_json == b'true',
        )

    # reconstruct the schema from the column configs
    # without the custom metadata (stringify_json flags are preserved)
    schema = _arrow_schema_from_column_configs(column_configs)

    return column_configs, schema


def _table_rows_to_arrow_batch(
    rows: list[dict],
    column_configs: dict[str, _ArrowColumnConfig],
    schema: pa.Schema,
    logger=None,
) -> pa.RecordBatch:
    """Convert a batch of flattened rows using a fixed Arrow schema."""
    pa, _, _ = require_pyarrow()
    arrays = [
        _normalize_arrow_column(
            [row.get(column_name) for row in rows],
            config,
            column_name,
            logger=logger,
        )
        for column_name, config in column_configs.items()
    ]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _is_nested_type(dtype: pa.DataType) -> bool:
    """Check if a PyArrow type is nested."""
    pa, _, _ = require_pyarrow()
    return pa.types.is_nested(dtype)


def _get_csv_compatible_schema(schema: pa.Schema) -> pa.Schema:
    """Convert schema to CSV-compatible format by changing nested types to strings."""
    pa, _, _ = require_pyarrow()
    new_fields = []
    for field in schema:
        if _is_nested_type(field.type):
            new_fields.append(pa.field(field.name, pa.string(), field.nullable))
        else:
            new_fields.append(field)
    return pa.schema(new_fields)


def _stringify_nested_columns(
    batch: pa.RecordBatch, csv_schema: pa.Schema
) -> pa.RecordBatch:
    """Convert nested columns (list, struct) in a batch to JSON strings."""
    pa, _, _ = require_pyarrow()
    new_columns = []
    for i, column in enumerate(batch.columns):
        if _is_nested_type(batch.schema.field(i).type):
            stringified = pa.array(
                [
                    json.dumps(value, separators=(',', ':'))
                    if value is not None
                    else None
                    for value in column.to_pylist()
                ],
                type=pa.string(),
            )
            new_columns.append(stringified)
        else:
            new_columns.append(column)

    return pa.RecordBatch.from_arrays(new_columns, schema=csv_schema)


def generate_archives(
    manifest: list[ManifestEntry], required: dict | str, user_id: str, logger=None
) -> Iterable[dict]:
    """
    Yields entry archive dict using the manifest and required fields one at a time.
    """

    # set up required reader
    required_reader = RequiredReader(
        required=required,
        resolve_inplace=True,
        user=User(user_id=user_id),
    )

    # arrange entries by upload_id
    manifest_dict = defaultdict(list)
    for entry in manifest:
        manifest_dict[entry.upload_id].append(entry.entry_id)

    for upload_id, entry_ids in manifest_dict.items():
        if logger:
            logger.info(f'processing upload_id: {upload_id}')

        upload_files = UploadFiles.get(upload_id)
        if upload_files is None:
            if logger:
                logger.info(f'no upload files found for upload_id: {upload_id}')
            continue

        for entry_id in entry_ids:
            entry = {'entry_id': entry_id, 'upload_id': upload_id}
            try:
                with upload_files.read_archive(entry_id) as upload_archive:
                    entry['archive'] = required_reader.read(
                        upload_archive, entry_id, upload_id
                    )
                    if entry['archive'] is None:
                        continue
                    yield entry
            except Exception as e:
                if logger:
                    logger.error(
                        'failed to read entry archive',
                        entry_id=entry_id,
                        upload_id=upload_id,
                        exc_info=e,
                    )


def generate_table_rows(
    manifest: list[ManifestEntry], required: dict | str, user_id: str, logger=None
) -> Iterable[FlatEntryArchive]:
    """
    Yields table row using the manifest and required fields one at a time.
    """
    archives = generate_archives(manifest, required, user_id, logger)

    all_unhandled_keys: set[str] = set()
    for archive in archives:
        entry_id = archive['entry_id']
        upload_id = archive['upload_id']
        try:
            table_row = _flatten_entry_archive(
                archive['archive'],
                entry_id,
                upload_id,
            )
            all_unhandled_keys.update(table_row.unhandled_keys)
            yield table_row
        except Exception as e:
            if logger:
                logger.error(
                    'failed to flatten archive '
                    f'(entry_id={entry_id} upload_id={upload_id})',
                    exc_info=e,
                )
    if all_unhandled_keys and logger:
        if logger:
            logger.warning(
                f'Unhandled keys ({len(all_unhandled_keys)}) while flattening '
                f'archives: {all_unhandled_keys}'
            )


def write_dicts_to_json(items: Iterable[dict], output_file_path: Path) -> int:
    first_item = True
    count = 0

    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write('[\n')

        for item in items:
            if not first_item:
                f.write(',\n')
            json.dump(item, f, separators=(',', ':'))
            count += 1
            first_item = False

        f.write('\n]')

    return count


def write_table_rows_to_ndjson(  # noqa: PLR0913, PLR0917
    manifest: list[ManifestEntry],
    required: str | dict[str, Any],
    user_id: str,
    output_file_path: Path,
    schema_file_path: Path,
    logger=None,
) -> None:
    if not output_file_path.suffix == '.ndjson':
        raise ValueError('ouput_file_path should have .ndjson extension.')
    column_configs: dict[str, _ArrowColumnConfig] = {}

    table_rows_with_columns_quantity_def = generate_table_rows(
        manifest, required, user_id, logger
    )

    with open(output_file_path, 'w', encoding='utf-8') as file:
        for table_row in table_rows_with_columns_quantity_def:
            # accumulate only the schema information
            for column, quantity_def in table_row.columns_quantity_def.items():
                if column not in column_configs:
                    column_configs[column] = _quantity_to_arrow_column_config(
                        quantity_def
                    )

            # write the current row immediately
            json.dump(table_row.data_dict, file, separators=(',', ':'))
            file.write('\n')

    _write_table_schema(
        schema_file_path,
        _add_ids_and_order_column_configs(column_configs),
    )


def _tabular_output_file_format(output_file_path: Path) -> str:
    output_file_format = output_file_path.suffix.lstrip('.')
    if output_file_format not in {'parquet', 'csv'}:
        raise ValueError('Unsupported output file format. Please use parquet or csv.')
    return output_file_format


def _create_tabular_writer(
    output_file_path: Path,
    output_file_format: str,
    schema: pa.Schema,
):
    _, pcsv, pq = require_pyarrow()
    if output_file_format == 'csv':
        return pcsv.CSVWriter(output_file_path, schema)
    return pq.ParquetWriter(
        output_file_path,
        schema,
        compression='zstd',
        compression_level=3,
        use_dictionary=True,
    )


def _write_tabular_table(writer, table: pa.Table, output_file_format: str) -> None:
    if output_file_format == 'parquet':
        writer.write_table(table, row_group_size=table.num_rows)
    else:
        # pcsv.CSVWriter.write_table does not support row_group_size
        writer.write_table(table)


def write_table_rows_to_tabular_file(  # noqa: PLR0913
    table_rows_file_path: Path,
    output_file_path: Path,
    schema_file_path: Path,
    *,
    max_buffer_bytes: int = 64 * 1024 * 1024,
    max_buffer_rows: int = 512,
    logger=None,
) -> int:
    """
    Stream flattened rows from NDJSON into bounded Parquet or CSV batches.

    The fixed union schema and column normalization settings are loaded from an Arrow
    IPC schema sidecar created during NDJSON generation. Parsed rows are flushed when
    either their count reaches ``max_buffer_rows`` or their encoded NDJSON input reaches
    ``max_buffer_bytes``.

    A single row is always accepted. If its NDJSON input is larger than
    ``max_buffer_bytes``, it is logged and written immediately.
    """
    pa, _, _ = require_pyarrow()
    output_file_format = _tabular_output_file_format(output_file_path)
    if max_buffer_bytes < 1:
        raise ValueError('max_buffer_bytes must be at least 1.')
    if max_buffer_rows < 1:
        raise ValueError('max_buffer_rows must be at least 1.')

    column_configs, arrow_schema = _read_table_schema(schema_file_path)
    output_schema = (
        _get_csv_compatible_schema(arrow_schema)
        if output_file_format == 'csv'
        else arrow_schema
    )

    buffered_rows: list[dict] = []
    buffered_input_bytes = 0
    count = 0

    with (
        open(table_rows_file_path, 'rb') as input_file,
        _create_tabular_writer(
            output_file_path, output_file_format, output_schema
        ) as writer,
    ):

        def flush_batch() -> None:
            nonlocal buffered_input_bytes, count  # update from flush
            if not buffered_rows:
                return

            batch = _table_rows_to_arrow_batch(
                buffered_rows,
                column_configs,
                arrow_schema,
                logger=logger,
            )
            if output_file_format == 'csv':
                batch = _stringify_nested_columns(batch, output_schema)
            table = pa.Table.from_batches([batch], schema=output_schema)
            _write_tabular_table(writer, table, output_file_format)
            if logger:
                logger.info(
                    f'Flushed {len(buffered_rows)} rows, {buffered_input_bytes} bytes'
                )
            count += table.num_rows
            buffered_rows.clear()
            buffered_input_bytes = 0

        for line in input_file:
            row_input_bytes = len(line)

            if buffered_rows and (
                len(buffered_rows) >= max_buffer_rows
                or buffered_input_bytes + row_input_bytes > max_buffer_bytes
            ):
                flush_batch()

            standard_row = json.loads(line)
            if not isinstance(standard_row, dict):
                raise ValueError('Each table row must be a JSON object.')
            buffered_rows.append(standard_row)
            buffered_input_bytes += row_input_bytes

            if row_input_bytes > max_buffer_bytes:
                if logger:
                    logger.warning(
                        'Tabular row exceeds the NDJSON input buffer size and '
                        'will be written immediately.',
                        entry_id=standard_row.get('entry_id'),
                        output_file_format=output_file_format,
                        row_input_bytes=row_input_bytes,
                        max_buffer_bytes=max_buffer_bytes,
                    )
                flush_batch()
            elif (
                len(buffered_rows) >= max_buffer_rows
                or buffered_input_bytes >= max_buffer_bytes
            ):
                flush_batch()

        flush_batch()

    return count


def worker_process_initializer() -> None:
    from nomad.infrastructure import setup_mongo

    setup_mongo()
