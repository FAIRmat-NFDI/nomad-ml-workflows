import importlib
import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache

import json_stream
import pandas as pd
from nomad.app.v1.models.models import User
from nomad.archive.required import RequiredReader
from nomad.datamodel.datamodel import EntryArchive, EntryData
from nomad.files import UploadFiles
from nomad.metainfo import data_type as nomad_data_type
from nomad.metainfo.metainfo import Quantity, Reference, Section

from nomad_ml_workflows.actions.export_entries.models import ManifestEntry

try:
    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
except ImportError as e:
    raise ImportError(
        'pyarrow is required. Install with: pip install nomad-ml-workflows[cpu-action]'
    ) from e

IGNORED_KEYS = ['m_def', 'm_def_id', 'm_ref_archives']


@dataclass(frozen=True)
class _ArrowColumnConfig:
    arrow_type: pa.DataType
    stringify_json: bool = False


@dataclass
class _FlattenEntryContext:
    row: dict[str, str | int | float | bool | dict | list | None]
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
    quantity_type = quantity_def.type

    if isinstance(quantity_type, Reference):
        return _ArrowColumnConfig(pa.string(), stringify_json=True)

    if isinstance(quantity_type, nomad_data_type.JSON | nomad_data_type.Any):
        return _ArrowColumnConfig(pa.string(), stringify_json=True)

    try:
        standard_type = quantity_type.standard_type()
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


def _infer_arrow_column(values: list, column_name: str) -> pa.Array:
    """Infer non-schema column types, falling back to deterministic JSON text."""
    try:
        array = pa.array(values)
        return array
    except (
        pa.ArrowInvalid,
        pa.ArrowNotImplementedError,
        pa.ArrowTypeError,
        OverflowError,
        TypeError,
        ValueError,
    ):
        try:
            return pa.array(
                [_json_stringify(value) for value in values], type=pa.string()
            )
        except (TypeError, ValueError) as error:
            raise ValueError(
                f'Cannot infer or JSON-encode column {column_name!r}.'
            ) from error


def _store_leaf_value(row: dict, col: str, value):
    """Store a value as one opaque DataFrame cell."""
    row[col] = value


def _flatten_section(
    section_data: dict,
    section_def: Section,
    prefix: str,
    context: _FlattenEntryContext,
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

    for quantity_name, quantity_def in section_def.all_quantities.items():
        if quantity_name not in section_data:
            continue
        handled_keys.add(quantity_name)
        col = f'{_join_path(prefix, quantity_name)}#{section_def.qualified_name()}'
        context.columns_quantity_def.setdefault(col, quantity_def)
        _store_leaf_value(context.row, col, section_data[quantity_name])

    for sub_section_name, sub_section_def in section_def.all_sub_sections.items():
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
                    context,
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
                context,
            )

    # Add the unhandled keys to a list. These point to the obsolete data in the
    # archives that do not correspond to a subsection/quantity in the current schema
    for key, _ in section_data.items():
        if key not in [*handled_keys, *IGNORED_KEYS]:
            context.unhandled_keys.append(
                f'{prefix}.{key}#{section_def.qualified_name()}'
            )


def archive_to_row(archive: dict) -> tuple[dict, dict[str, Quantity]]:
    """
    Flatten archive dict into a row dict. Also returns column definition.

    `archive` argument should have the following structure, where `archive` corresponds
    to serialization of EntryArchive::
        {
            "entry_id": str,
            "archive": {
                "results": dict,
                "metadata": dict,
                "data": dict,
                "processing_log": list[Any],
                ...
            },
        }
    """
    if not isinstance(archive, dict):
        raise ValueError('archive must be a dictionary (JSON object).')
    if 'archive' not in archive or 'entry_id' not in archive:
        raise ValueError('archive and entry_id keys are required.')
    if not isinstance(archive['archive'], dict):
        raise ValueError('archive[archive] must be a dictionary (JSON object).')

    columns_quantity_def: dict[str, Quantity] = {}
    context = _FlattenEntryContext(
        row={'entry_id': archive['entry_id']},  # Always include entry_id as a column
        columns_quantity_def=columns_quantity_def,
        unhandled_keys=[],
    )
    _flatten_section(
        section_data=archive['archive'],
        section_def=EntryArchive.m_def,
        prefix='',
        context=context,
    )
    columns_quantity_def.update(context.columns_quantity_def)

    return context.row, columns_quantity_def


def _archives_to_rows(  # noqa: PLR0912
    archives: list[dict] | dict, logger=None
) -> tuple[list[dict], dict[str, Quantity]]:
    """
    Flatten list of archive dicts into list of row dicts. Also returns column definition
    and a count of unhandled archive dict keys.

    Each element in the `archives` list should have the following structure, where
    `archive` key corresponds to serialization of EntryArchive::
        {
            "entry_id": str,
            "archive": {
                "results": dict,
                "metadata": dict,
                "data": dict,
                "processing_log": list[Any],
                ...
            },
        }
    """
    if isinstance(archives, dict):
        archives = [archives]
    elif not isinstance(archives, list):
        raise ValueError(
            'Input must be a dictionary (JSON object) or a list of dictionaries '
            '(JSON objects)'
        )

    rows = []
    columns_quantity_def: dict[str, Quantity] = {}
    failed_to_flatten = []
    unhandled_key_entry_ids: dict[str, set[str]] = {}
    for item in archives:
        if item is None:
            if logger:
                logger.warning('Encountered a None item.')
            continue
        if not isinstance(item, dict):
            raise ValueError('Input must be a dictionary (JSON object).')
        if 'archive' not in item or 'entry_id' not in item:
            raise ValueError('archive and entry_id keys are required.')
        if not isinstance(item['archive'], dict):
            raise ValueError('archive value must be a dictionary (JSON object).')

        context = _FlattenEntryContext(
            row={'entry_id': item['entry_id']},  # Always include entry_id as a column
            columns_quantity_def=columns_quantity_def,
            unhandled_keys=[],
        )
        try:
            _flatten_section(
                section_data=item['archive'],
                section_def=EntryArchive.m_def,
                prefix='',
                context=context,
            )
            rows.append(context.row)
            columns_quantity_def.update(context.columns_quantity_def)
            for key in context.unhandled_keys:
                unhandled_key_entry_ids.setdefault(key, set()).add(item['entry_id'])
        except Exception as e:
            if logger:
                logger.warning(
                    f'failed to flatten archive (entry_id={item["entry_id"]})',
                    exc_info=e,
                )
            failed_to_flatten.append(item['entry_id'])

    if logger:
        # cummulative logging
        for key, entry_ids_set in unhandled_key_entry_ids.items():
            entry_ids = list(entry_ids_set)
            logger.warning(
                f'unhandled key {key} (num_entries={len(entry_ids)})',
                entry_ids=entry_ids,
            )
        if failed_to_flatten:
            logger.warning(
                f'failed to flatten archives (num_entries={len(failed_to_flatten)})',
            )

    return rows, columns_quantity_def


def _ordered_columns(columns) -> list[str]:
    """Put entry_id first and sort all remaining columns alphabetically."""
    return sorted(columns, key=lambda column: (column != 'entry_id', column))


def archives_to_dataframe(archives: list[dict] | dict, logger=None) -> pd.DataFrame:
    """
    Convert serialized NOMAD entries into a flattened pandas DataFrame.

    Archive traversal follows NOMAD metainfo subsections, while quantities remain
    opaque cell values. Concrete ``m_def`` values are used to resolve schemas, and
    pandas infers the resulting column dtypes from the flattened rows.
    The ``entry_id`` column is returned first, followed by all other columns in
    alphabetical order.

    Args:
        archives: A serialized entry dictionary or list of entry dictionaries. Each
            entry may contain an ``archive`` dictionary and optional top-level data.

    Returns:
        A flattened DataFrame.
    """
    rows, _ = _archives_to_rows(archives, logger=logger)

    df = pd.DataFrame(rows)
    sorted_df = df.reindex(_ordered_columns(df.columns), axis=1)

    return sorted_df


def archives_to_arrow_table(archives: list[dict] | dict, logger=None) -> pa.Table:
    """
    Convert serialized NOMAD entries into a schema-typed Arrow table.

    Entries are flattened using the same metainfo-aware traversal as
    :func:`archives_to_dataframe`. Schema-backed columns receive explicit Arrow types
    derived from their quantity definitions, including nested list dimensions. Values
    are safely cast when necessary; JSON, ``Any``, references, complex numbers, and
    unsupported custom datatypes are stored as deterministic JSON strings. Columns
    without a NOMAD quantity definition use Arrow inference with the same JSON fallback.
    The ``entry_id`` column is returned first, followed by all other columns in
    alphabetical order.

    Args:
        archives: A serialized entry dictionary or list of entry dictionaries. Each
            entry may contain an ``archive`` dictionary and optional top-level data.

    Returns:
        A schema-typed Arrow table.
    """
    rows, columns_quantity_def = _archives_to_rows(archives, logger)
    column_names = _ordered_columns({column for row in rows for column in row})

    arrays = []
    for column_name in column_names:
        values = [row.get(column_name) for row in rows]
        quantity_def = columns_quantity_def.get(column_name)
        if quantity_def is None:
            array = _infer_arrow_column(values, column_name)
        else:
            config = _quantity_to_arrow_column_config(quantity_def)
            array = _normalize_arrow_column(
                values,
                config,
                column_name,
                logger=logger,
            )
        arrays.append(array)

    return pa.Table.from_arrays(arrays, names=column_names)


def _is_nested_type(dtype: pa.DataType) -> bool:
    """Check if a PyArrow type is nested."""
    return pa.types.is_nested(dtype)


def _get_csv_compatible_schema(schema: pa.Schema) -> pa.Schema:
    """Convert schema to CSV-compatible format by changing nested types to strings."""
    new_fields = []
    for field in schema:
        if _is_nested_type(field.type):
            new_fields.append(pa.field(field.name, pa.string(), field.nullable))
        else:
            new_fields.append(field)
    return pa.schema(new_fields)


def _stringify_nested_columns(batch: pa.RecordBatch) -> pa.RecordBatch:
    """Convert nested columns (list, struct) in a batch to JSON strings."""
    new_columns = []
    for i, column in enumerate(batch.columns):
        if _is_nested_type(batch.schema.field(i).type):
            # Convert each element to JSON string
            stringified = pa.array(
                [
                    json.dumps(val.as_py()) if val.as_py() is not None else None
                    for val in column
                ],
                type=pa.string(),
            )
            new_columns.append(stringified)
        else:
            new_columns.append(column)

    return pa.RecordBatch.from_arrays(
        new_columns, schema=_get_csv_compatible_schema(batch.schema)
    )


def write_parquet_file(path: str, data: list[dict], logger=None):
    """Writes a list of NOMAD entry dicts to a parquet file.

    Args:
        path (str): The path where the file will be saved.
        data (list[dict]): The list of NOMAD entry dicts to be written to the file.
    """
    if not path.endswith('parquet'):
        raise ValueError('Unsupported file format. Please use parquet.')

    table: pa.Table = archives_to_arrow_table(data, logger)
    with pq.ParquetWriter(
        path,
        table.schema,
        compression='snappy',  # snappy for faster write/read for individual files
        use_dictionary=True,
    ) as writer:
        writer.write_table(table)

    return table.num_rows


def write_json_file(path: str, data, logger=None):
    """Writes a JSON file with the given data.

    Args:
        path (str): The path where the file will be saved.
        data (list[dict]): The list of NOMAD entry dicts to be written to the file.
    """
    if not path.endswith('json'):
        raise ValueError('Unsupported file format. Please use json.')

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

    return len(data)


def merge_files(
    input_file_paths: list[str], output_file_format: str, output_file_path: str
):
    """Merges multiple Parquet or JSON files into a single file.

    Args:
        input_file_paths (list[str]): List of file paths to be merged.
        output_file_format (str): The format of the output file ('parquet', 'csv', or
            'json').
        output_file_path (str): Path of the merged output file.
    """
    if output_file_format == 'parquet':
        # Creates a logical dataset from the input files, not loading all data into
        # memory. Also, unifies the schema across the files.
        dataset = ds.dataset(input_file_paths, format='parquet')

        # Write the dataset to a single Parquet file in batches
        with pq.ParquetWriter(
            output_file_path,
            dataset.schema,
            compression='zstd',  # for better compression for merged file
            compression_level=3,
            use_dictionary=True,
        ) as writer:
            for batch in dataset.to_batches():
                writer.write_batch(batch)

    elif output_file_format == 'csv':
        # Creates a logical dataset from the input files, not loading all data into
        # memory. Also, unifies the schema across the files.
        # The batch files for `csv` are written in Parquet format for efficiency,
        # so we read them as Parquet here.
        dataset = ds.dataset(input_file_paths, format='parquet')

        # PyArrow CSV writer doesn't support nested types (list, struct, etc.)
        # Convert nested columns to JSON strings
        csv_schema = _get_csv_compatible_schema(dataset.schema)

        # Write the dataset to a single CSV file in batches
        with pcsv.CSVWriter(output_file_path, csv_schema) as writer:
            for batch in dataset.to_batches():
                csv_batch = _stringify_nested_columns(batch)
                writer.write_batch(csv_batch)

    elif output_file_format == 'json':

        def _json_stream_files(input_file_paths):
            """Generator that streams one entry dict at a time from multiple files."""
            for file_path in input_file_paths:
                with open(file_path, encoding='utf-8') as f:
                    data = json_stream.load(f)
                    yield from data

        # Write a single JSON file by streaming entry dicts and wrapping in a list
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write('[\n')
            first_item = True
            for item in _json_stream_files(input_file_paths):
                if not first_item:
                    f.write(',\n')
                # Convert transient json_stream object to standard Python types
                json.dump(json_stream.to_standard_types(item), f, indent=4)
                first_item = False
            f.write('\n]')

    else:
        raise ValueError('Unsupported file format. Please use parquet, csv, or json.')


def generate_archives(
    manifest: list[ManifestEntry], required: dict | str, user_id: str, logger=None
) -> Iterable[dict]:
    """Generates entry archive dicts from the manifest and required fields."""

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
                with upload_files.read_archive(entry_id) as archive:
                    entry['archive'] = required_reader.read(
                        archive, entry_id, upload_id
                    )
                    yield entry
            except Exception as e:
                logger.error(
                    'failed to read entry archive',
                    entry_id=entry_id,
                    upload_id=upload_id,
                    exc_info=e,
                )


def generate_table_rows(
    manifest: list[ManifestEntry], required: dict | str, user_id: str, logger=None
) -> Iterable[tuple[dict, dict[str, Quantity]]]:
    """
    Generates table rows from the manifest and required fields.
    """
    archives = generate_archives(manifest, required, user_id, logger)
    for archive in archives:
        try:
            yield archive_to_row(archive)
        except Exception as e:
            logger.error(
                'failed to generate table rows for archive',
                exc_info=e,
            )


def write_dicts_to_json(items: Iterable[dict], output_file_path: str) -> int:
    first_item = True
    count = 0

    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write('[\n')

        for item in items:
            if not first_item:
                f.write(',\n')
            json.dump(item, f, indent=2)
            count += 1
            first_item = False

        f.write('\n]')

    return count
