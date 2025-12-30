from enum import StrEnum

from pydantic import BaseModel, Field, NonNegativeInt

from .enums import Granularity
from .framework.beacon_info import BeaconInfo
from .framework.configuration import BeaconConfiguration
from .framework.endpoints_map import BeaconMap
from .framework.entry_types import EntryTypes
from .framework.filtering_term import FilteringTerms
from .framework.result_sets import CollectionsList, ResultSets
from .variant import VariantQueryParameters


class BeaconError(BaseModel):
    errorCode: int
    errorMessage: str | None = None


class Pagination(BaseModel):
    limit: NonNegativeInt | None = 10
    skip: NonNegativeInt | None = 0
    currentPage: str | None = None
    nextPage: str | None = None
    previousPage: str | None = None


class FilterOperator(StrEnum):
    EQUAL = "="
    NOT_EQUAL = "!"
    LESS = "<"
    LESS_OR_EQUAL = "<="
    GREATER = ">"
    GREATER_OR_EQUAL = ">="


class IncludeResponses(StrEnum):
    ALL = "ALL"
    HIT = "HIT"
    MISS = "MISS"
    NONE = "NONE"


class IndividualVariantParams(BaseModel):
    g_variant: VariantQueryParameters | None = None



class QueryFilter(BaseModel):
    id: str
    includeDescendantTerms: bool = True
    similarity: str | None = None  # In the spec; not used.
    operator: FilterOperator | None = None
    value: str | None = None
    scope: str | None = None


class BeaconQuery(BaseModel):
    # User Portal Discovery service sends variant parameters as
    # `requestParameters: list[VariantQueryParameters]`
    # https://github.com/GenomicDataInfrastructure/gdi-userportal-dataset-discovery-service/blob/main/src/main/openapi/beacon/individuals.yaml
    requestParameters: VariantQueryParameters | list[VariantQueryParameters] | None = None
    filters: list[QueryFilter] | None = None
    includeResultsetResponses: IncludeResponses | None = None
    pagination: Pagination | None = None
    requestedGranularity: Granularity | None = None
    testMode: bool | None = None


class BeaconQueryResponse(BaseModel):
    exists: bool
    numTotalResults: NonNegativeInt | None = None


class SchemaPerEntity(BaseModel):
    entityType: str
    schemaUrl: str = Field(..., alias="schema")


class ReceivedRequestSummary(BaseModel):
    apiVersion: str
    requestedSchemas: list[SchemaPerEntity]
    filters: list[QueryFilter] | None = None
    requestParameters: VariantQueryParameters | list[VariantQueryParameters] | None = None
    includeResultsetResponses: IncludeResponses | None = None
    pagination: Pagination
    requestedGranularity: Granularity
    testMode: bool | None = None


class RequestMeta(BaseModel):
    apiVersion: str = ""
    requestedSchemas: list[SchemaPerEntity] | None = None


class ResponseMeta(BaseModel):
    beaconId: str
    apiVersion: str
    receivedRequestSummary: ReceivedRequestSummary | None = None
    returnedSchemas: list[SchemaPerEntity]
    returnedGranularity: Granularity | None = None
    testMode: bool | None = None


class BeaconRequest(BaseModel):
    meta: RequestMeta
    query: BeaconQuery | None = None


class BeaconResponse(BaseModel):
    meta: ResponseMeta
    responseSummary: BeaconQueryResponse | None = None
    response: BeaconInfo | BeaconMap | FilteringTerms | EntryTypes | BeaconConfiguration | ResultSets | CollectionsList | None = None
    error: BeaconError | None = None
    info: dict[str, str | dict] | None = None
