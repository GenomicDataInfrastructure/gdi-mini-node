from logging import getLogger

import pyarrow.dataset as ds
from isoduration.parser import parse_duration
from isoduration.types import Duration

from ._parquet import PQ_INDIVIDUAL_PROPS_SCHEMA, PQ_VCF_INDIVIDUAL_SCHEMA, \
    parquet_filter_for_variants, read_parquet
from ..model.common import BeaconRequest, IncludeResponses
from ..model.framework.result_sets import ResultSet, ResultSets
from ..model.variant import VariantQueryParameters
from ..setup import BeaconSetup
from ...data import DATA
from ...data.registry import BeaconAssembly

"""Implementation for exposing Beacon individuals count (only for sensitive Beacon).

Here is the standard endpoint description:
https://github.com/ga4gh-beacon/beacon-v2/blob/v2.2.0/models/src/beacon-v2-default-model/individuals/endpoints.yaml

Here, request parameters "requestedSchema" and "filters" are ignored.
If "includeResultsetResponses" != "HIT", the response will be empty.
Only "skip" and "limit" are supported.

The client code that this endpoint tries to conform to:
https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/main/src/main/java/io/github/genomicdatainfrastructure/discovery/datasets/infrastructure/beacon/persistence/BeaconDatasetIdsCollector.java

The client makes a record-level request to obtain result-sets. The actual
individual count value is read from response.resultSets[].resultsCount.
Therefore, the server supports record-level response but with an empty array in
resultSets[].results.
"""

EMPTY_ARRAY = []
_log = getLogger(__name__)


class IndividualFilter:
    """Filter for the phenotypic properties of individuals.

    The filter parameters are obtained from the request parameters.
    An instance is created even if there are no filters provided.
    In that case, the matches_all() method returns True.

    Currently, there are only two filters supported:
    * sex – the ontology value for male (NCIT:C20197) and female (NCIT:C16576).
    * age - ISO 8601 period + comparator (<, >, <=, >=, =, !)

    Note that comparing ISO 8601 periods can be imprecise.
    """

    def __init__(
            self, sex: str | None, age: Duration | None, operator: str | None,
    ):
        self.sex = self._convert_from_ontology_key(sex)
        self.age = f"{operator}{age}" if age and operator else None
        self._age_compare = self._age_matcher(age, operator)

    def matches_all(self) -> bool:
        return self.sex is None and self._age_compare is None

    def has_age_filter(self) -> bool:
        return self._age_compare is not None

    def matches_age(self, file_age: str) -> bool:
        if self._age_compare is None:
            return True

        if file_age is None or not file_age.startswith("P"):
            return False

        try:
            value = parse_duration(file_age)
            return self._age_compare(value)
        except ValueError as e:
            _log.warning(
                f"Invalid ISO 8601 Period [{file_age}] encountered in "
                f"individuals.parquet file: {repr(e)}"
            )
            return False

    @staticmethod
    def _convert_from_ontology_key(provided_value: str) -> str | None:
        if provided_value is None or len(provided_value) == 0:
            return None
        if provided_value == "NCIT:C20197":
            return "M"
        if provided_value == "NCIT:C16576":
            return "F"
        return "UNKNOWN"

    @staticmethod
    def _age_matcher(age: Duration | None, operator: str | None):
        if age is None or operator is None:
            return None
        if operator == "<":
            return lambda x: age < x
        if operator == ">":
            return lambda x: age > x
        if operator == "<=":
            return lambda x: age <= x
        if operator == ">=":
            return lambda x: age >= x
        if operator == "=":
            return lambda x: age == x
        if operator == "!":
            return lambda x: age != x
        assert False, f"Operator not supported: [{operator}]"


def get_individuals_count(
        request: BeaconRequest, setup: BeaconSetup,
) -> ResultSets:
    ## PARAMS ##

    # No data for testMode requests:
    if request.query.testMode:
        _log.info("Returning empty results due to request.query.testMode=true")
        return ResultSets()

    # To make sure that we don't support any other result-set mode than HIT:
    include_mode = request.query.includeResultsetResponses
    if include_mode and include_mode != IncludeResponses.HIT:
        _log.info(f"Returning empty results due to "
                  f"request.query.includeResultsetResponses='%s'", include_mode)
        return ResultSets()

    # Variant filters are returned only when found in request:
    params = resolve_variant_filter(request)
    if params is not None and not params.has_sufficient_values():
        params = None
    if params is not None and params.has_unsupported_values():
        _log.warning("Returning empty results due to unsupported request "
                     "parameters: %s", params.model_dump(exclude_none=True))
        return ResultSets()

    # Filters object is returned when there are no validation issues:
    filters = resolve_filters(request)
    if filters is None:
        _log.info("Returning empty results due to issues in "
                  "request.query.filters")
        return ResultSets()

    page = request.query.pagination
    limit = page.limit if page and page.limit is not None else 10
    skip = page.skip if page and page.skip is not None else 0

    ## SEARCH ##

    # If variant-filter is missing, just read the individuals.parquet file.
    if params is None:
        results = get_results_from_individuals_parquet(filters, skip, limit)
    else:
        # Filter variants-file only when there are variant-parameters provided.
        results = get_results_from_variants(params, filters, skip, limit, setup)

    _log.info("Individual variant results contains %d datasets for query: %s",
              len(results.resultSets), request.model_dump(exclude_none=True))

    return results


def resolve_variant_filter(
        request: BeaconRequest,
) -> VariantQueryParameters | None:
    """
    Extracts VariantQueryParameters from the request parameters.
    If missing, returns None.

    As referenced above, the client sends variant-parameters as a JSON object
    within a list. However, these parameters can also be omitted.
    """
    params = request.query.requestParameters
    if isinstance(params, list) and len(params) > 0:
        params = params[0]
    if isinstance(params, VariantQueryParameters) and params.has_values():
        return params
    return None


def resolve_filters(request: BeaconRequest) -> IndividualFilter | None:
    """Resolves individual-level filtering parameters.

    https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/main/src/main/resources/META-INF/resources/beacon-filters.json

    This solution supports only 2 types of filters (sex and age).
    When other filters are provided, this method returns None to notify about
    inappropriate filters, which also leads to an empty response.

    In addition, this method requires the scope to be equal to "individual".
    The value is used by the client:
    https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/main/src/main/java/io/github/genomicdatainfrastructure/discovery/datasets/infrastructure/beacon/persistence/BeaconIndividualsRequestMapper.java#L34
    It is enforced here to be sure that the filter applies to individuals.
    Other values lead to mismatch.

    Filters are optional. Even when no filters are provided, this method returns
    an IndividualFilter. None is returned only on validation failure.
    """

    # NOTE: not sure if this "diseases.ageOfOnset.iso8601duration" should be
    # supported as the name refers to diseases but our data does not support
    # diseases yet. So this is a subject to be reviewed and decided later.
    # It was initially included with the meaning: individual's age during the
    # extraction of the genetic sample.

    supported_scope = "individual"
    supported_filters = {"sex", "diseases.ageOfOnset.iso8601duration"}

    sex = None
    age_value = None
    age_operator = None
    filters = request.query.filters

    if filters is not None:
        for item in filters:
            if item.id not in supported_filters:
                _log.warning(f"Unsupported filter [{item.id}]")
                return None

            if item.scope != supported_scope:
                _log.warning(
                    f"Unexpected scope [{item.scope}] for filter [{item.id}]")
                return None

            if item.id == "sex":
                sex = item.value

            elif item.id == "diseases.ageOfOnset.iso8601duration":
                try:
                    # Note: validness of the operator is ensured by Pydantic.
                    age_operator = item.operator
                    age_value = parse_duration(item.value)
                except ValueError as e:
                    shortened = item.value
                    if len(shortened) > 40:
                        l = len(shortened)
                        shortened = f"{shortened[:40]}... (length={l})"
                    _log.warning(
                        "Incoming 'diseases.ageOfOnset.iso8601duration' value '"
                        f"{shortened}"
                        "'could not be parsed into ISO 8601 duration: "
                        f"{repr(e)}"
                    )
                    # No results will be returned due to this error:
                    return None

    return IndividualFilter(sex, age_value, age_operator)


def get_results_from_individuals_parquet(
        filters: IndividualFilter,
        skip: int,
        limit: int,
) -> ResultSets:
    """
    Performs the entire search on "individual.parquet" files from different
    datasets. This method is used only when variant-filtering is omitted.

    Args:
        filters: IndividualFilter for filtering individuals.
        skip: Number of matching datasets to skip before collecting results.
        limit: Maximum number of results to collect.

    Returns:
        Results within ResultSets. No individual-records included.
    """
    _log.info("Retrieving individuals where (sex=%s, age=%s)",
              filters.sex, filters.age)

    results = ResultSets()
    dataset_match_count = 0

    for dataset_id, parquet_files in DATA.sensitive_beacon.get_dataset_individuals().items():
        count = filter_individuals(parquet_files[0], set(), filters)
        if count == 0:
            continue

        dataset_match_count += 1

        if dataset_match_count <= skip:
            continue

        results.resultSets.append(ResultSet(
            id=dataset_id,
            resultsCount=count,
            results=EMPTY_ARRAY,
        ))
        if len(results.resultSets) >= limit:
            break

    return results


def get_results_from_variants(
        params: VariantQueryParameters,
        filters: IndividualFilter,
        skip: int,
        limit: int,
        setup: BeaconSetup,
) -> ResultSets:
    """
    Performs the entire the search in two stages per each dataset:
    1. find matching individuals by variant ("individuals-chrX.Y.parquet")
    2. run another filtering ("individuals.parquet") on individuals from step 1
       to exclude non-matching sex and age properties.

    This method is used only when variant-filtering is provided.

    Args:
        params: VariantQueryParameters for finding individuals by variant.
        filters: IndividualFilter for filtering individuals.
        skip: Number of matching datasets to skip before collecting results.
        limit: Maximum number of results to collect.
        setup: BeaconSetup for filtering individuals.

    Returns:
        Results within ResultSets. No individual-records included.
    """
    # Obtain datasets and their matching variant position file:
    dataset_files = DATA.sensitive_beacon.get_dataset_individuals(
        BeaconAssembly(params.assemblyId),
        params.referenceName,
        params.start[0],
    )

    results = ResultSets()
    dataset_match_count = 0

    for dataset_id, parquet_files in dataset_files.items():
        individuals_count = filter_individuals_by_variant(
            params, filters, parquet_files[1], parquet_files[0])

        # Censoring enables to filter out rare individuals with rare variants.
        # Default censoring threshold is 1, which essentially does not censor.
        individuals_count = setup.censor_count(individuals_count)

        if individuals_count is None:
            continue

        dataset_match_count += 1

        if dataset_match_count <= skip:
            continue

        results.resultSets.append(ResultSet(
            id=dataset_id,
            resultsCount=individuals_count,
            results=EMPTY_ARRAY,
        ))
        if len(results.resultSets) >= limit:
            break

    return results


def filter_individuals_by_variant(
        params: VariantQueryParameters,
        filters: IndividualFilter,
        variants_parquet_file: str,
        individuals_parquet_file: str,
) -> int | None:
    """Calculate the number of individuals, that matching the given
    filter-criteria, in given Parquet file.

    Args:
        params: VariantQueryParameters for finding individuals by variant.
        filters: IndividualFilter for filtering individuals.
        variants_parquet_file: The target Parquet file for variant data.
        individuals_parquet_file: The target Parquet file for individual data.

    Returns:
        the count of matching individuals, or None on no matches and failures.
    """

    # Parquet file filtering expression:
    row_matcher = parquet_filter_for_variants(params)
    table = read_parquet(
        variants_parquet_file,
        PQ_VCF_INDIVIDUAL_SCHEMA,
        row_matcher,
        "INDIVIDUALS",
    )

    if table is None or table.num_rows == 0:
        return None

    # We just expect one matching row here, so the loop runs only once.
    for row in table.to_pylist():
        indices = parse_range(row["INDIVIDUALS"])
        return filter_individuals(individuals_parquet_file, indices, filters)
    return None


def parse_range(ranges_str: str) -> set[int]:
    """Parses the string-value from INDIVIDUALS column and returns the numbers
    in a set. The string is expected to contain comma-delimited numbers and
    number-ranges, which will be converted to multiple numbers."""

    results = set()
    for item in ranges_str.split(","):
        if "-" in item:
            start, end = item.split("-")
            for i in range(int(start), int(end) + 1):
                results.add(i)
        else:
            results.add(int(item))
    return results


def filter_individuals(
        parquet_file: str,
        individual_indices: set[int],
        filters: IndividualFilter,
) -> int | None:
    """Returns the number of individuals that match the given filter-criteria.

    Args:
        parquet_file: The target "individuals.parquet" file.
        individual_indices: to restrict the set of individuals, this set
            contains indices (target values for INDEX column); an empty includes
            all individuals in the file.
        filters: additional filters (sex, age-range)

    Returns:
        the count of matching rows.
    """

    # The count can be computed from the list of selected individuals
    # when the filter includes everyone:
    if filters.matches_all() and len(individual_indices) > 0:
        _log.debug("Returning individuals count as len(INDIVIDUALS): %d",
                   len(individual_indices))
        return len(individual_indices)

    row_matcher = None
    if len(individual_indices) > 0:
        row_matcher = ds.field("INDEX").isin(individual_indices)
    if filters.sex:
        sex_value_matcher = ds.field("SEX") == filters.sex
        if row_matcher is None:
            row_matcher = sex_value_matcher
        else:
            row_matcher = row_matcher & sex_value_matcher

    # Loading the pre-filtered dataset from the file:
    table = read_parquet(
        parquet_file, PQ_INDIVIDUAL_PROPS_SCHEMA, row_matcher, "AGE")

    if table is None or table.num_rows == 0:
        return None

    match_count = 0
    if not filters.has_age_filter():
        match_count = table.num_rows
    else:
        # The value for AGE is an ISO 8601 Period string, which needs to be
        # parsed and compared to the filter value.
        for row in table.to_pylist():
            if not filters.matches_age(row["AGE"]):
                continue
            match_count += 1

    _log.debug(f"Matched individuals: {match_count}")
    return match_count if match_count > 0 else None


__all__ = ["get_individuals_count"]
