from fastapi import Request
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError

from ..model.common import (
    BeaconQuery,
    BeaconRequest,
    FilterOperator,
    IncludeResponses,
    Pagination,
    QueryFilter,
)
from ..model.enums import Granularity
from ..model.variant import VariantQueryParameters
from ..setup import BeaconSetup


def resolve_variants_request(
        request: Request,
        beacon_setup: BeaconSetup,
        validate: bool = True,
) -> BeaconRequest | None:
    """Collects BeaconRequest data from URL parameters and caches them in the
    request state.

    Note that this data-initialisation works only for GET requests.
    Therefore, POST-endpoints need to explicitly cache their payloads under
    `request.state.BeaconRequest`.

    Entity defaults to 'genomicVariant', as we don't support others.
    GET-endpoint uses BeaconVariantsRequestDep for injecting the request data.
    """
    try:
        beacon_request: BeaconRequest = request.state.BeaconRequest
        return beacon_request
    except AttributeError:
        variant_params = parse_variant_params(request, validate)
        query = BeaconQuery(
            requestParameters=variant_params,
            filters=parse_filters(request),
            includeResultsetResponses=parse_include_responses(request),
            pagination=parse_pagination(request, validate),
            requestedGranularity=parse_granularity(request),
            testMode=parse_test_mode(request),
        )
        auth_header = request.headers.get("Authorization")
        request.state.BeaconRequest = beacon_setup.request_for_query(auth_header, query)
        return request.state.BeaconRequest


def parse_include_responses(request: Request) -> IncludeResponses | None:
    value = request.query_params.get("includeResultsetResponses")
    return IncludeResponses(value) if value in IncludeResponses else None


def parse_granularity(request: Request) -> Granularity | None:
    value = request.query_params.get("requestedGranularity")
    if value is None:
        value = request.query_params.get("granularity")
    return Granularity(value) if value in Granularity else None


def parse_test_mode(request: Request) -> bool | None:
    value = request.query_params.get("testMode")
    return value == "true" if value == "true" or value == "false" else None


def parse_pagination(request: Request, validate: bool = True) -> Pagination | None:
    skip = request.query_params.get("skip")
    limit = request.query_params.get("limit")
    params = {}
    if skip:
        params["skip"] = skip
    if limit:
        params["limit"] = limit
    if params:
        if validate:
            try:
                return Pagination(**params)
            except ValidationError as e:
                raise RequestValidationError(e.errors(), body=params) from e
        else:
            return Pagination.model_construct(**params)
    return None


def parse_filters(request: Request) -> list[QueryFilter] | None:
    value = request.query_params.get("filters")
    if value is None or value.strip() == "":
        return None

    values = [item.strip() for item in value.split(",") if item.strip()]

    filters = []
    for item in values:
        has_operator = False

        # Check for and parse operators, if present:
        for operator in FilterOperator:
            operator_pos = item.find(operator)
            if operator_pos > 1:
                field_id = item[0:operator_pos].rstrip(":").replace("_", ":")
                value = item[operator_pos + 1 :].lstrip()
                filters.append(QueryFilter(id=field_id, operator=operator, value=value))
                has_operator = True
                break

        # Only ID expected without a comparison operator:
        if not has_operator:
            filters.append(QueryFilter(id=item))

    return filters


def parse_variant_params(
    request: Request, validate: bool = True
) -> VariantQueryParameters | None:
    if not request.url.path.endswith("/g_variants") or request.method == "POST":
        return None

    params = {}
    for name in VariantQueryParameters.model_fields:
        if name in request.query_params:
            params[name] = request.query_params[name]

    if validate:
        try:
            return VariantQueryParameters(**params)
        except ValidationError as e:
            raise RequestValidationError(e.errors(), body=params) from e
    else:
        return VariantQueryParameters.model_construct(**params)
