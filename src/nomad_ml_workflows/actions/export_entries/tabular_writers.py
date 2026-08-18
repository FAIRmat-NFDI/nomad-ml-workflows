from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from types import TracebackType

    import pyarrow as pa


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


class TabularWriter(Protocol):
    """Common lifecycle for tabular writers."""

    def __enter__(self) -> TabularWriter: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...

    def write_table(self, table: pa.Table) -> Path: ...

    def close(self) -> None: ...


class CSVWriter:
    """Write every table through one persistent Arrow CSV writer."""

    def __init__(self, output_path: Path, schema: pa.Schema):
        if output_path.suffix != '.csv':
            raise ValueError('output_path must be a CSV file.')

        self._output_path = output_path
        self._schema = schema
        self._writer = None
        self._closed = False

    def __enter__(self) -> CSVWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.close()
        elif self._writer is not None:
            self._writer.close()
            self._closed = True
        else:
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError('Cannot write to a closed CSV writer.')

    def _get_writer(self):
        if self._writer is None:
            _, pcsv, _ = require_pyarrow()
            self._writer = pcsv.CSVWriter(self._output_path, self._schema)
        return self._writer

    def write_table(self, table: pa.Table) -> Path:
        self._ensure_open()
        self._get_writer().write_table(table)
        return self._output_path

    def close(self) -> None:
        if self._closed:
            return
        self._get_writer().close()
        self._closed = True


class ParquetWriter:
    """Write each table through a short-lived writer into one dataset part."""

    def __init__(self, output_path: Path, schema: pa.Schema):
        if output_path.suffix != '.parquet':
            raise ValueError('output_path must be a Parquet dataset directory.')

        output_path.mkdir()
        self._output_path = output_path
        self._schema = schema
        self._part_number = 0
        self._closed = False

    def __enter__(self) -> ParquetWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.close()
        else:
            # Each part writer has already closed, so no resource remains open.
            self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise ValueError('Cannot write to a closed Parquet writer.')

    def write_table(self, table: pa.Table) -> Path:
        self._ensure_open()
        _, _, pq = require_pyarrow()
        part_file_path = self._output_path / f'part-{self._part_number:05d}.parquet'

        with pq.ParquetWriter(
            part_file_path,
            self._schema,
            compression='zstd',
            compression_level=3,
            use_dictionary=True,
        ) as writer:
            if table.num_rows:
                writer.write_table(table, row_group_size=table.num_rows)

        self._part_number += 1
        return part_file_path

    def close(self) -> None:
        if self._closed:
            return
        if self._part_number == 0:
            pa, _, _ = require_pyarrow()
            self.write_table(pa.Table.from_batches([], schema=self._schema))
        self._closed = True


def create_tabular_writer(
    output_path: Path,
    schema: pa.Schema,
) -> TabularWriter:
    """Create the writer matching the output artifact suffix."""
    if output_path.suffix == '.csv':
        return CSVWriter(output_path, schema)
    if output_path.suffix == '.parquet':
        return ParquetWriter(output_path, schema)
    raise ValueError('Unsupported output file format. Please use parquet or csv.')
