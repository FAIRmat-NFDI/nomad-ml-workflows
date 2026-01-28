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
    """Merges multiple Parquet, CSV, or JSON files into a single file.

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
        # Additional timestamp parser for ISO with microseconds and timezone with colon
        # (e.g., 2023-10-05T14:48:00.000000+00:00)
        # This is used in NOMAD `nomad.metainfo.metainfo.Datetime` serialization
        convert_options = pcsv.ConvertOptions(
            timestamp_parsers=[
                '%Y-%m-%dT%H:%M:%S.%f%z',
            ]
        )
        # Read more rows for schema inference to avoid null type inference errors
        # CAUTION: this might not scale for very large files
        read_options = pcsv.ReadOptions(
            block_size=100 << 20,  # 100 MB blocks for better schema inference
        )
        # Creates a logical dataset from the input CSV files, not loading all data into
        # memory. Also, unifies the schema across the files.
        dataset = ds.dataset(
            input_file_paths,
            format=ds.CsvFileFormat(
                convert_options=convert_options,
                read_options=read_options,
            ),
        )

        # Write the dataset to a single CSV file in batches
        with pa.csv.CSVWriter(output_file_path, dataset.schema) as writer:
            for batch in dataset.to_batches():
                writer.write_batch(batch)

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
