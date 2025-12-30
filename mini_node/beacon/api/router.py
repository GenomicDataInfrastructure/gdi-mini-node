from typing import Callable

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.status import HTTP_401_UNAUTHORIZED

from ..config import BeaconContext
from ..model.common import BeaconRequest, BeaconResponse
from ..model.enums import EntityType
from ..model.framework.service_info import ServiceInfo
from ..service.allele_freq import find_datasets_allele_frequencies
from ..service.datasets import get_datasets
from ..service.individuals import get_individuals_count
from ..setup import BeaconSetup


def beacon_router(beacon_context: BeaconContext):
    setup = BeaconSetup(beacon_context)
    base_path = beacon_context.service.base_path

    if beacon_context.aggregated:
        tag = "beacon-aggregated"
    else:
        tag = "beacon-sensitive"

    router = APIRouter(tags=[tag], prefix=base_path)

    @router.get("/", response_model_exclude_none=True)
    @router.get("/info", response_model_exclude_none=True)
    def info(request: Request) -> BeaconResponse:
        return BeaconResponse(
            meta=setup.info_response_meta(EntityType.INFO),
            response=setup.beacon_info(str(request.base_url)),
        )

    @router.get("/service-info", response_model_exclude_none=True)
    def service_info() -> ServiceInfo:
        return setup.service_info

    @router.get("/configuration", response_model_exclude_none=True)
    def configuration() -> BeaconResponse:
        return BeaconResponse(
            meta=setup.info_response_meta(EntityType.CONFIGURATION),
            response=setup.configuration,
        )

    @router.get("/entry_types", response_model_exclude_none=True)
    def entry_types() -> BeaconResponse:
        return BeaconResponse(
            meta=setup.info_response_meta(EntityType.ENTRY_TYPES),
            response=setup.entry_types,
        )

    @router.get("/map", response_model_exclude_none=True)
    def map(request: Request) -> BeaconResponse:
        return BeaconResponse(
            meta=setup.info_response_meta(EntityType.MAP),
            response=setup.map(str(request.base_url)),
        )

    @router.get("/filtering_terms", response_model_exclude_none=True)
    def filtering_terms() -> BeaconResponse:
        return BeaconResponse(
            meta=setup.info_response_meta(EntityType.FILTERING_TERM),
            response=setup.filtering_terms,
        )

    # Exposed endpoints if aggregated-data Beacon is enabled:
    if beacon_context.aggregated:
        @router.post(
            "/g_variants",
            response_model=None,
            response_model_exclude_none=True,
        )
        async def search_variants(
                request: Request, body: BeaconRequest,
        ) -> JSONResponse | PlainTextResponse:
            return process_request(request, body, setup, handle_af_lookup)

        @router.post("/datasets", response_model=None,
                     response_model_exclude_none=True)
        async def search_datasets(
                request: Request, body: BeaconRequest,
        ) -> JSONResponse | PlainTextResponse:
            return process_request(request, body, setup, handle_datasets)

    # Exposed endpoints if "regular" sensitive data Beacon is enabled:
    else:
        @router.post("/individuals", response_model=None,
                     response_model_exclude_none=True)
        async def search_individuals(
                request: Request, body: BeaconRequest,
        ) -> JSONResponse | PlainTextResponse:
            return process_request(request, body, setup, handle_individuals)

    return router, setup


def process_request(
        request: Request,
        body: BeaconRequest,
        setup: BeaconSetup,
        handler: Callable[[BeaconRequest, BeaconSetup], JSONResponse],
) -> JSONResponse | PlainTextResponse:
    # Record the body in the state for error handlers (_create_error_response):
    request.state.BeaconRequest = body

    # Verify access:
    auth_mode = setup.authenticate(request.headers.get("Authorization"))
    if auth_mode is not None:
        return PlainTextResponse(
            f"This resource requires {auth_mode} authentication.",
            status_code=HTTP_401_UNAUTHORIZED,
            headers={"WWW-Authenticate": auth_mode},
        )

    # Invoke the endpoint-specific implementation:
    return handler(body, setup)


def handle_af_lookup(request: BeaconRequest, setup: BeaconSetup):
    result_sets = find_datasets_allele_frequencies(request)
    payload = setup.response(request, result_sets, EntityType.GENOMIC_VARIANT)
    return JSONResponse(payload.model_dump(exclude_none=True))


def handle_datasets(request: BeaconRequest, setup: BeaconSetup):
    datasets = get_datasets(request)
    payload = setup.collection_response(request, datasets, EntityType.DATASET)
    return JSONResponse(payload.model_dump(exclude_none=True))


def handle_individuals(request: BeaconRequest, setup: BeaconSetup):
    resultsets = get_individuals_count(request, setup)
    payload = setup.response(request, resultsets, EntityType.INDIVIDUAL)
    return JSONResponse(payload.model_dump(exclude_none=True))
