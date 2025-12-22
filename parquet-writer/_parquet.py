import random
from logging import getLogger
from os.path import isfile, join
from time import time
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from isoduration import parse_duration
from pyarrow import csv

_log = getLogger(__name__)

POS_DIVIDER = 10_000_000
PQ_SORT_ORDER = [("POS", "ascending")]

PQ_VCF_AF_SCHEMA = pa.schema(
    [
        ("POS", pa.int32()),
        ("REF", pa.string()),
        ("ALT", pa.string()),
        ("VT", pa.string()),
        ("POPULATION", pa.string()),
        ("AF", pa.float64()),
        ("AC", pa.int32()),
        ("AC_HET", pa.int32()),
        ("AC_HOM", pa.int32()),
        ("AC_HEMI", pa.int32()),
        ("AN", pa.int32()),
    ]
)

PQ_VCF_INDIVIDUAL_SCHEMA = pa.schema(
    [
        ("POS", pa.int32()),
        ("REF", pa.string()),
        ("ALT", pa.string()),
        ("VT", pa.string()),
        ("INDIVIDUALS", pa.string()),  # 2,7-9,20,35-55 (refs to INDEX column)
    ]
)

PQ_INDIVIDUAL_PROPS_SCHEMA = pa.schema(
    [
        ("INDEX", pa.int32()),  # 1, 2, 3, ...
        ("SEX", pa.string()),  # "M" or "F"
        ("AGE", pa.string()),  # "P25Y4M" -> 25 years, 4 months
    ]
)


class ParquetVcfWriter:
    """Helper class for writing one or more Parquet files with variant data.

    It ensures:
    - the positions are in sequence (possibly with gaps);
    - there is no attempt to overwrite an existing Parquet file;

    """

    def __init__(self, output_dir, schema: pa.Schema, file_prefix: str) -> None:
        self._output_dir = output_dir
        self.parquet_files = set()
        self._file_prefix = file_prefix
        self._ordering = pq.SortingColumn.from_ordering(schema, PQ_SORT_ORDER)

        # Used internally for tracking progress:
        self._start_group = time()
        self._buf = ColumnBuffer(schema)
        self._chr_id: str | None = None
        self._group_num: int | None = None
        self._pos = 0
        self._finished_chr_ids: set[str] = set()
        self.total_row_count = 0

    def close(self) -> None:
        self._write_table()

    def add_row(self, row: dict[str, Any]) -> None:
        self._buf.append(row)

    def at_chr_pos(self, chr_id: str, pos: int) -> None:
        """Based on the current chromosome position, detects if it's time to
        write current batch of rows into a Parquet file; and records the new
        position."""

        if self._chr_id is None:
            self._chr_id = chr_id

        if self._chr_id == chr_id and self._pos > pos:
            raise RuntimeError(
                f"Variants not in order for chromosome '{chr_id}' "
                f"(previous position {self._pos} > current pos {pos})",
            )
        self._pos = pos

        group_num = pos // POS_DIVIDER
        if self._group_num is None:
            self._group_num = group_num

        if self._chr_id == chr_id and self._group_num == group_num:
            # No change: same chromosome, same group.
            return

        _log.debug("CHROM %s->%s, GROUP %d->%d, writing tables",
                   self._chr_id, chr_id, self._group_num, group_num)
        self._write_table()

        if self._chr_id != chr_id:
            self._finished_chr_ids.add(self._chr_id)
            _log.debug("Finished chromosomes: %s", self._finished_chr_ids)

        self._chr_id = chr_id
        self._group_num = group_num

    def _write_table(self) -> None:
        row_count = len(self._buf)
        if row_count == 0:
            return

        file_path = join(
            self._output_dir,
            f"{self._file_prefix}{self._chr_id}.{self._group_num}.parquet",
        )
        _log.debug("Writing %d rows to file: %s", row_count, file_path)

        if isfile(file_path):
            _log.warning("File %s already exists, overwriting it", file_path)

        pq.write_table(
            self._buf.to_table(),
            file_path,
            sorting_columns=self._ordering,
            write_page_index=True,
            compression="zstd",
            compression_level=19,
            write_page_checksum=True,
        )

        self.total_row_count += row_count
        self.parquet_files.add(file_path)
        _log.info(
            "Processed CHROM=%s GROUP=%d with %d rows in %.2f seconds: %s",
            self._chr_id,
            self._group_num,
            row_count,
            time() - self._start_group,
            file_path,
        )

        # Prepare for next batch:
        self._start_group = time()
        self._buf.clear()


class ColumnBuffer:
    """Helper class for collection and storing table-row values per column, as
    Parquet tables are column-based (and not row-based)."""

    def __init__(self, schema: pa.Schema):
        self.schema = schema
        self._columns = schema.names
        self._data = {c: [] for c in self._columns}

    def append(self, row: dict[str, Any]) -> None:
        for c in self._columns:
            self._data[c].append(row.get(c))

    def clear(self) -> None:
        for values in self._data.values():
            values.clear()

    def __len__(self) -> int:
        for values in self._data.values():
            return len(values)
        return 0

    def to_table(self) -> pa.Table:
        return pa.table(
            [pa.array(self._data[c]) for c in self._columns],
            schema=self.schema,
        )


def write_individuals_parquet(
        input_csv_path: str, dest_dir: str, samples: list[str],
) -> None:
    if input_csv_path == "RANDOM":
        table = _random_individuals_data(samples)
    else:
        table = _prepare_individuals_data_from_csv(input_csv_path, samples)

    # Save the table to the Parquet file:
    file_path = join(dest_dir, "individuals.parquet")
    _log.info("Writing %d rows to file: %s", table.num_rows, file_path)

    if isfile(file_path):
        _log.warning("File %s already exists, overwriting it", file_path)

    sort_order = [("INDEX", "ascending")]
    sorting = pq.SortingColumn.from_ordering(table.schema, sort_order)
    pq.write_table(
        table,
        file_path,
        sorting_columns=sorting,
        write_page_index=True,
        compression="zstd",
        compression_level=19,
        write_page_checksum=True,
    )


def _prepare_individuals_data_from_csv(
        input_csv_path: str, samples: list[str]
) -> pa.Table:
    if not isfile(input_csv_path):
        raise RuntimeError("File not found: %s", input_csv_path)

    table = csv.read_csv(
        input_csv_path,
        csv.ReadOptions(column_names=PQ_INDIVIDUAL_PROPS_SCHEMA),
    )

    _log.info("Read %d rows from CSV file: %s", table.num_rows, input_csv_path)

    index_values = []
    for sample in table.column("SAMPLE").slice():
        idx = samples.index(sample)
        if idx < 0:
            raise RuntimeError(
                f"VCF SAMPLE [{sample}] not found in file: {input_csv_path}")
        index_values.append(samples.index(sample))

    # Replace column SAMPLE with the new INDEX column:
    sample_col_index = table.column_names.index("SAMPLE")
    table.set_column(sample_col_index, "INDEX", [index_values])

    # Sort the table by INDEX in ascending order:
    table.sort_by("INDEX")
    return table


def _random_individuals_data(samples: list[str]) -> pa.Table:
    options_sex = ["M", "F"]
    index_values = []
    sex_values = []
    age_values = []

    _log.info("Generating data for %d individuals from VCF.", len(samples))

    for i in range(len(samples)):
        age_year = random.randint(22, 88)
        age_month = random.randint(0, 11)
        age = f"P{age_year}Y"
        age += f"{age_month}M" if age_month > 0 else ""

        index_values.append(i)
        sex_values.append(random.choice(options_sex))
        age_values.append(age)

    return pa.table(
        [pa.array(index_values), pa.array(sex_values), pa.array(age_values)],
        schema=PQ_INDIVIDUAL_PROPS_SCHEMA,
    )


def print_individuals_summary(file_path: str) -> None:
    if not isfile(file_path):
        raise RuntimeError("File not found: %s", file_path)

    _log.debug("Starting to read Parquet file: %s", file_path)
    table = pq.read_table(file_path,
                          schema=PQ_INDIVIDUAL_PROPS_SCHEMA, columns=["AGE"])

    _log.debug("Removing NULL values from the AGE column.")
    table.drop_null()

    _log.debug("Sorting AGE values.")
    table.sort_by("AGE")

    individual_count = table.num_rows
    _log.debug("Count of individuals (num_rows): %d.", individual_count)

    rows = table.slice(offset=0, length=1).to_pylist()
    value = rows[0].get("AGE")
    _log.debug("Minimum age: '%s'.", value)
    min_age = parse_duration(value).date.years

    rows = table.slice(offset=table.num_rows-1, length=1).to_pylist()
    value = rows[0].get("AGE")
    _log.debug("Maximum age: '%s'.", value)
    max_age = parse_duration(value).date.years

    print("individual_count:", individual_count)
    print("min_age:", min_age)
    print("max_age:", max_age)
