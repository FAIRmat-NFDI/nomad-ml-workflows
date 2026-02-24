import json

import json_stream
from nomad.utils import dict_to_dataframe

try:
    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
except ImportError as e:
    raise ImportError(
        'pyarrow is required. Install with: pip install nomad-ml-workflows[cpu-action]'
    ) from e


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


def write_parquet_file(path: str, data: list[dict]):
    """Writes a list of NOMAD entry dicts to a parquet file.

    Args:
        path (str): The path where the file will be saved.
        data (list[dict]): The list of NOMAD entry dicts to be written to the file.
    """
    if not path.endswith('parquet'):
        raise ValueError('Unsupported file type. Please use parquet.')

    df = dict_to_dataframe(data)

    table = pa.Table.from_pandas(df)
    with pq.ParquetWriter(
        path,
        table.schema,
        compression='snappy',  # snappy for faster write/read for individual files
        use_dictionary=True,
    ) as writer:
        writer.write_table(table)


def write_csv_file(path: str, data: list[dict]):
    """Writes a list of NOMAD entry dicts to a CSV file.

    Args:
        path (str): The path where the file will be saved.
        data (list[dict]): The list of NOMAD entry dicts to be written to the file.
    """
    if not path.endswith('csv'):
        raise ValueError('Unsupported file type. Please use csv.')

    df = dict_to_dataframe(data)

    df.to_csv(path, index=False, mode='w', header=True)


def write_json_file(path: str, data: list[dict]):
    """Writes a list of NOMAD entry dicts to a JSON file.

    Args:
        path (str): The path where the file will be saved.
        data (list[dict]): The list of NOMAD entry dicts to be written to the file.
    """
    if not path.endswith('json'):
        raise ValueError('Unsupported file type. Please use json.')

    with open(path, 'w') as f:
        json.dump(data, f, indent=4)


def merge_files(
    input_file_paths: list[str], output_file_type: str, output_file_path: str
):
    """Merges multiple Parquet or JSON files into a single file.

    Args:
        input_file_paths (list[str]): List of file paths to be merged.
        output_file_type (str): The type of the output file ('parquet', 'csv', or
            'json').
        output_file_path (str): Path of the merged output file.
    """
    if output_file_type == 'parquet':
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

    elif output_file_type == 'csv':
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

    elif output_file_type == 'json':

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
        raise ValueError('Unsupported file type. Please use parquet, csv, or json.')
