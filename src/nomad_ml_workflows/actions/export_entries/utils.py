from nomad.utils import dict_to_dataframe

try:
    import pyarrow as pa
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
        mode (str, optional): The write mode, either 'overwrite' or 'append'.
            Defaults to 'overwrite'.
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
        mode (str, optional): The write mode, either 'overwrite' or 'append'.
            Defaults to 'overwrite'.
    """
    if not path.endswith('csv'):
        raise ValueError('Unsupported file type. Please use CSV.')

    df = dict_to_dataframe(data)

    df.to_csv(path, index=False, mode='w', header=True)


def write_json_file(path: str, data: list[dict]):
    """Writes a list of NOMAD entry dicts to a JSON file.

    Args:
        path (str): The path where the file will be saved.
        data (list[dict]): The list of NOMAD entry dicts to be written to the file.
    """
    if not path.endswith('json'):
        raise ValueError('Unsupported file type. Please use JSON.')

    import json

    with open(path, 'w') as f:
        json.dump(data, f, indent=4)


def merge_files(input_file_paths: list[str], output_file_path: str):
    """Merges multiple Parquet, CSV, or JSON files into a single file.

    Args:
        input_file_paths (list[str]): List of file paths to be merged.
        output_file_path (str): Path of the merged output file.
    """
    if output_file_path.endswith('parquet'):
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

    elif output_file_path.endswith('csv'):
        import pandas as pd

        dataframes = []
        for file_path in input_file_paths:
            df = pd.read_csv(file_path)
            dataframes.append(df)
        combined_df = pd.concat(dataframes, ignore_index=True)
        combined_df.to_csv(output_file_path, index=False)
    elif output_file_path.endswith('json'):
        import json

        combined_data = []
        for file_path in input_file_paths:
            with open(file_path, encoding='utf-8') as f:
                data = json.load(f)
                # extend the combined entry list with entry list from each file
                combined_data.extend(data)
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(combined_data, f, indent=4)
    else:
        raise ValueError('Unsupported file type. Please use parquet, CSV, or JSON.')
