import json
import importlib
from functools import lru_cache

import json_stream
import pandas as pd
from nomad.datamodel.datamodel import EntryArchive
from nomad.metainfo.metainfo import MSectionReference, Section

try:
    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
except ImportError as e:
    raise ImportError(
        'pyarrow is required. Install with: pip install nomad-ml-workflows[cpu-action]'
    ) from e


def _join_path(prefix: str, key: str) -> str:
    return f'{prefix}.{key}' if prefix else key


@lru_cache(maxsize=256)
def _resolve_section_def(m_def: str | None) -> Section | None:
    if not m_def:
        return None

    if '.' in m_def:
        package_name, section_name = m_def.rsplit('.', 1)
        try:
            module = importlib.import_module(package_name)
        except ImportError:
            module = None

        if module is not None:
            section = getattr(module, section_name, None)
            section_def = getattr(section, 'm_def', None)
            if isinstance(section_def, Section):
                return section_def

    try:
        resolved = MSectionReference().normalize(m_def).m_resolved()
    except Exception:
        return None

    return resolved if isinstance(resolved, Section) else None


def _flatten_generic(value, prefix: str, row: dict):
    """Flatten generic nested data while keeping non-dict lists as leaf values."""
    if isinstance(value, dict):
        for key, item in value.items():
            _flatten_generic(item, _join_path(prefix, key), row)
    elif isinstance(value, list):
        if all(isinstance(item, dict) for item in value):
            for index, item in enumerate(value):
                _flatten_generic(item, _join_path(prefix, str(index)), row)
        else:
            row[prefix] = value
    else:
        row[prefix] = value


def _store_leaf_value(row: dict, prefix: str, value):
    """Store a value as one opaque DataFrame cell."""
    row[prefix] = value


def _flatten_section(
    section_data: dict, section_def: Section, prefix: str, row: dict
):
    """
    Flatten one serialized NOMAD section using its metainfo definition.

    The traversal only recurses through declared subsections. Every quantity is
    treated as an opaque leaf value, regardless of whether it stores a scalar,
    array, nested list, or JSON payload.
    """
    handled_keys = set()

    for quantity_name in section_def.all_quantities:
        if quantity_name not in section_data:
            continue
        handled_keys.add(quantity_name)
        _store_leaf_value(
            row,
            _join_path(prefix, quantity_name),
            section_data[quantity_name],
        )

    for sub_section_name, sub_section_def in section_def.all_sub_sections.items():
        if sub_section_name not in section_data:
            continue

        handled_keys.add(sub_section_name)
        sub_section_value = section_data[sub_section_name]
        sub_section_prefix = _join_path(prefix, sub_section_name)
        child_section_def = sub_section_def.sub_section.m_resolved()

        if sub_section_value is None:
            _store_leaf_value(row, sub_section_prefix, None)
            continue

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
                    row,
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
                row,
            )

    # Keep serialization-only fields such as m_def/m_def_id as leaf values instead of
    # flattening them structurally. This preserves the "quantities opaque, only
    # subsections recursive" rule for keys outside the resolved section definition.
    for key, value in section_data.items():
        if key in handled_keys:
            continue
        _store_leaf_value(row, _join_path(prefix, key), value)


def archives_to_dataframe(archives: list[dict] | dict) -> pd.DataFrame:
    """
    Convert exported entries to a DataFrame.

    Each input item is expected to be a dict containing a serialized `archive`
    entry alongside optional top-level metadata fields. For example,

    ```python
    {
        entry_id: ...,
        upload_id: ...,
        archive: {...}  # serialized EntryArchive instance 1
    }
    ```

    The `archive` value is flattened using `EntryArchive` metainfo definitions;
    recursion only follows subsections and every quantity becomes one DataFrame cell.
    This keeps the DataFrame aligned with NOMAD section boundaries across the whole
    archive, including `archive.data` where plugin-provided `m_def` values are
    used to resolve the concrete section definition.
    """
    if isinstance(archives, dict):
        archives = [archives]
    elif not isinstance(archives, list):
        raise ValueError(
            'Input must be a dictionary (JSON object) or a list of dictionaries '
            '(JSON objects)'
        )

    rows = []
    for item in archives:
        if not isinstance(item, dict):
            raise ValueError(
                'Input must be a dictionary (JSON object).'
            )

        row = {}
        for key, value in item.items():
            if key != 'archive':
                _flatten_generic(value, key, row)
                continue

            # `archive` always starts from the fixed EntryArchive definition.
            _flatten_section(value, EntryArchive.m_def, key, row)

        rows.append(row)

    df = pd.DataFrame(rows)
    sorted_df = df.reindex(sorted(df.columns), axis=1)

    return sorted_df


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


def _make_dataframe_arrow_compatible(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stringify object columns that cannot be written to Parquet as-is.
    """
    normalized_df = df.copy()

    for column_name in normalized_df.columns:
        column = normalized_df[column_name]
        if column.dtype != 'object':
            continue

        values = column.dropna().tolist()
        if not values:
            continue

        try:
            arrow_array = pa.array(values)
            if pa.types.is_struct(arrow_array.type) and len(arrow_array.type) == 0:
                # Parquet cannot write empty struct columns and PyArrow raises
                # `ArrowNotImplementedError` in this case.
                raise pa.ArrowNotImplementedError
        except (
            pa.ArrowInvalid,
            pa.ArrowTypeError,
            pa.ArrowNotImplementedError,
            TypeError,
            ValueError,
        ):
            normalized_df[column_name] = column.map(
                lambda value: (
                    json.dumps(value, sort_keys=True)
                    if value is not None
                    and not (isinstance(value, float) and pd.isna(value))
                    else None
                )
            )

    return normalized_df


def write_parquet_file(path: str, data: list[dict]):
    """Writes a list of NOMAD entry dicts to a parquet file.

    Args:
        path (str): The path where the file will be saved.
        data (list[dict]): The list of NOMAD entry dicts to be written to the file.
    """
    if not path.endswith('parquet'):
        raise ValueError('Unsupported file format. Please use parquet.')

    df = _make_dataframe_arrow_compatible(archives_to_dataframe(data))

    table = pa.Table.from_pandas(df)
    with pq.ParquetWriter(
        path,
        table.schema,
        compression='snappy',  # snappy for faster write/read for individual files
        use_dictionary=True,
    ) as writer:
        writer.write_table(table)


def write_json_file(path: str, data: list[dict]):
    """Writes a list of NOMAD entry dicts to a JSON file.

    Args:
        path (str): The path where the file will be saved.
        data (list[dict]): The list of NOMAD entry dicts to be written to the file.
    """
    if not path.endswith('json'):
        raise ValueError('Unsupported file format. Please use json.')

    with open(path, 'w') as f:
        json.dump(data, f, indent=4)


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
