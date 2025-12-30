from logging import getLogger

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from ..model.variant import VariantQueryParameters
from ...data import DATA

_log = getLogger(__name__)

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


def parquet_filter_for_variants(params: VariantQueryParameters):
    match_pos = ds.field("POS") == params.start[0]
    match_ref = pc.field("REF") == params.referenceBases
    match_alt = pc.field("ALT") == params.alternateBases
    match_vt = pc.field("VT") == (params.variantType or "SNP")
    return match_pos & match_ref & match_alt & match_vt


def read_parquet(
        parquet_file: str,
        schema: pa.Schema,
        row_matcher,
        fetch_column: str | None = None,
) -> pa.Table | None:
    """Loads a pre-filtered dataset from the given Parquet file."""
    try:
        DATA.forget_issues_with(parquet_file)

        _log.debug(f"Reading Parquet file: {parquet_file}")
        file_dataset = ds.dataset(parquet_file, schema)

        if row_matcher is not None:
            file_dataset = file_dataset.filter(row_matcher)

        columns = [fetch_column] if fetch_column else None
        return file_dataset.to_table(columns=columns)
    except Exception as e:
        DATA.record_issues_with(parquet_file, e)
        _log.exception("Failed to read Parquet file", e)
        return None


__all__ = [
    "PQ_INDIVIDUAL_PROPS_SCHEMA",
    "PQ_VCF_INDIVIDUAL_SCHEMA",
    "parquet_filter_for_variants",
    "read_parquet",
]
