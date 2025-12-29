from logging import getLogger
from uuid import uuid1
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

from ..model.allele_freq import (
    AlleleFreqResult,
    FrequencyInPopulations,
    Identifiers,
    LegacyVariation,
    Number,
    PopulationFrequency,
    SequenceInterval,
    SequenceLocation,
)
from ..model.common import BeaconRequest
from ..model.variant import VariantQueryParameters
from ...data import DATA
from ...data.registry import BeaconAssembly
from ._parquet import parquet_filter_for_variants

"""Implementation for variant lookup (only for aggregated Beacon).

Here is the standard endpoint description:
https://github.com/ga4gh-beacon/beacon-v2/blob/v2.2.0/models/src/beacon-v2-default-model/genomicVariations/endpoints.yaml

Here, only following request parameters are supported:
* pagination: skip, limit
* assemblyId
* referenceName
* start
* referenceBases
* alternateBases
* variantType (defaults to 'SNP')

The response is limited to minimal set of fields as listed in: AlleleFreqResult.

The story behind this solution:
"Proposal for populating AF Beacon with aggregated data from GoE"
https://docs.google.com/document/d/1LLzp6zZT3fSM1XxOXHuRqwJje1v726Z2/edit
"""

_log = getLogger(__name__)


@dataclass
class AFVariant:
    assembly: BeaconAssembly
    chrom: str
    pos: int
    ref: str
    alt: str
    variantType: str


@dataclass
class AFRow:
    population: str
    af: float
    ac: int
    ac_het: int
    ac_hom: int
    ac_hemi: int
    an: int


def find_datasets_allele_frequencies(
        request: BeaconRequest,
) -> dict[str, list[AlleleFreqResult]]:
    """Searches for matching variants in Parquet files and returns a dictionary
    of dataset IDs and its matching allele frequencies (per cohort).
    Currently, this implementation performs file readings in a single thread.
    """

    params = request.query.requestParameters
    if params is None or params.is_not_sufficient():
        return {}

    assembly = BeaconAssembly(params.assemblyId)

    dataset_files = DATA.aggregated_beacon.get_dataset_files(
        assembly, params.referenceName, params.start[0])

    page = request.query.pagination
    limit = page.limit if page and page.limit is not None else 10
    skip = page.skip if page and page.skip is not None else 0

    results = {}
    dataset_match_count = 0

    for dataset_id, parquet_file in dataset_files.items():
        skip_details = dataset_match_count < skip
        allele_frequencies = find_af(request, parquet_file, skip_details,
                                     assembly, params.referenceName)
        if allele_frequencies == False:
            continue

        dataset_match_count += 1

        if isinstance(allele_frequencies, AlleleFreqResult):
            results[dataset_id] = allele_frequencies
            if len(results) >= limit:
                break

    return results


def find_af(
        request: BeaconRequest, parquet_file: str, skip_details: bool,
        assembly_id: BeaconAssembly, chrom: str,
) -> AlleleFreqResult | bool:
    table = _read_parquet(request, parquet_file)

    if table is None:
        return False

    if skip_details:
        _log.debug("Skip details requested")
        return True

    af_rows = []
    af_variant = None
    for row_dict in table.to_pylist():
        if af_variant is None:
            af_variant = AFVariant(
                assembly=assembly_id,
                chrom=chrom,
                pos=row_dict.get("POS"),
                ref=row_dict.get("REF"),
                alt=row_dict.get("ALT"),
                variantType=row_dict.get("VT"),
            )
        af_row = AFRow(
            population=row_dict.get("POPULATION"),
            af=row_dict.get("AF"),
            ac=row_dict.get("AC"),
            ac_het=row_dict.get("AC_HET"),
            ac_hom=row_dict.get("AC_HOM"),
            ac_hemi=row_dict.get("AC_HEMI"),
            an=row_dict.get("AN"),
        )
        af_rows.append(af_row)

    _log.debug("af rows: %s", len(af_rows))
    return _result(af_variant, af_rows)


def _read_parquet(request: BeaconRequest, parquet_file: str) -> pa.Table | None:
    query = request.query.requestParameters
    if not isinstance(query, VariantQueryParameters):
        _log.warning("Request parameters not what expected:", type(query))
        return None

    # Parquet file filtering expression:
    # Note that the position in the Parquet file is already 0-based (VCF POS-1)
    row_matcher = parquet_filter_for_variants(query)

    # Loading the pre-filtered dataset from the file:
    try:
        DATA.forget_issues_with(parquet_file)
        file_dataset = ds.dataset(parquet_file, schema=PQ_VCF_AF_SCHEMA)
        file_dataset = file_dataset.filter(row_matcher)
        table: pa.Table = file_dataset.to_table()
    except Exception as e:
        DATA.record_issues_with(parquet_file, e)
        return None

    if table.num_rows == 0:
        _log.debug(
            "No matching Parquet rows in [%s] for allele-freq query: %s",
            parquet_file,
            query,
        )
        return None

    _log.info(
        "Found %d matching row(s) in [%s] for allele-freq query: %s",
        table.num_rows,
        parquet_file,
        query,
    )
    return table


def _result(variant: AFVariant, af_rows: list[AFRow]) -> AlleleFreqResult:
    return AlleleFreqResult(
        identifiers=_identifiers(variant),
        variantInternalId=_variant_internal_id(variant.ref, variant.alt),
        variation=_legacy_variation(variant),
        frequencyInPopulations=_frequencies(af_rows),
    )


def _identifiers(variant: AFVariant) -> Identifiers:
    ref_seq_id = REF_SEQ.get(variant.assembly, {}).get(variant.chrom, "")
    hgvs_id = f"{ref_seq_id}:g.{variant.pos + 1}{variant.ref}>{variant.alt}"
    return Identifiers(genomicHGVSId=hgvs_id)


def _variant_internal_id(ref: str, alt: str) -> str:
    # Follows the internal ID format used in beacon2-ri-tools-v2:
    return str(uuid1()) + ":" + str(ref) + ":" + str(alt)


def _legacy_variation(variant: AFVariant) -> LegacyVariation:
    seq_pos = variant.pos + 1  # The position in the Parquet file is 0-based.
    seq_id = f"HGVSid:{variant.chrom}:g.{seq_pos}{variant.ref}>{variant.alt}"
    seq_start = Number(value=variant.pos)
    seq_end = Number(value=variant.pos + len(variant.ref))

    return LegacyVariation(
        location=SequenceLocation(
            sequence_id=seq_id,
            interval=SequenceInterval(start=seq_start, end=seq_end),
        ),
        referenceBases=variant.ref,
        alternateBases=variant.alt,
        variantType=variant.variantType,
    )


def _frequencies(af_rows: list[AFRow]) -> list[FrequencyInPopulations]:
    pop_frequencies = []
    for af_row in af_rows:
        pop_frequencies.append(PopulationFrequency(
            population=af_row.population,
            alleleFrequency=af_row.af,
            alleleCount=af_row.ac,
            alleleCountHomozygous=af_row.ac_hom,
            alleleCountHeterozygous=af_row.ac_het,
            alleleCountHemizygous=af_row.ac_hemi,
            alleleNumber=af_row.an,
        ))

    return [
        FrequencyInPopulations(
            numberOfPopulations=len(pop_frequencies),
            populations=pop_frequencies,
        )
    ]


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

REF_SEQ = {
    # https://www.ncbi.nlm.nih.gov/datasets/genome/GCA_000001405.14/
    "GRCh37": {
        "1": "NC_000001.10",
        "2": "NC_000002.11",
        "3": "NC_000003.11",
        "4": "NC_000004.11",
        "5": "NC_000005.9",
        "6": "NC_000006.11",
        "7": "NC_000007.13",
        "8": "NC_000008.10",
        "9": "NC_000009.11",
        "10": "NC_000010.10",
        "11": "NC_000011.9",
        "12": "NC_000012.11",
        "13": "NC_000013.10",
        "14": "NC_000014.8",
        "15": "NC_000015.9",
        "16": "NC_000016.9",
        "17": "NC_000017.10",
        "18": "NC_000018.9",
        "19": "NC_000019.9",
        "20": "NC_000020.10",
        "21": "NC_000021.8",
        "22": "NC_000022.10",
        "X": "NC_000023.10",
        "Y": "NC_000024.9",
        "M": "NC_001807.4",
    },
    # https://www.ncbi.nlm.nih.gov/datasets/genome/GCF_000001405.40/
    "GRCh38": {
        "1": "NC_000001.11",
        "2": "NC_000002.12",
        "3": "NC_000003.12",
        "4": "NC_000004.12",
        "5": "NC_000005.10",
        "6": "NC_000006.12",
        "7": "NC_000007.14",
        "8": "NC_000008.11",
        "9": "NC_000009.12",
        "10": "NC_000010.11",
        "11": "NC_000011.10",
        "12": "NC_000012.12",
        "13": "NC_000013.11",
        "14": "NC_000014.9",
        "15": "NC_000015.10",
        "16": "NC_000016.10",
        "17": "NC_000017.11",
        "18": "NC_000018.10",
        "19": "NC_000019.10",
        "20": "NC_000020.11",
        "21": "NC_000021.9",
        "22": "NC_000022.11",
        "X": "NC_000023.11",
        "Y": "NC_000024.10",
        "M": "NC_012920.1",
    },
}

__all__ = ["find_datasets_allele_frequencies"]
