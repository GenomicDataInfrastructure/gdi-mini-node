from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException

from mini_node.beacon.api.error import BeaconErrorResponseHandler


def add_endpoints(app: FastAPI):
    from ..data.config import beacon_aggregated, beacon_sensitive

    if beacon_aggregated is None and beacon_sensitive is None:
        return

    from .api.router import beacon_router

    aggregated_setup = None
    sensitive_setup = None

    if beacon_aggregated is not None:
        router, aggregated_setup = beacon_router(beacon_aggregated)
        app.include_router(router)

    if beacon_sensitive is not None:
        router, sensitive_setup = beacon_router(beacon_sensitive)
        app.include_router(router)

    h = BeaconErrorResponseHandler(aggregated_setup, sensitive_setup)
    app.add_exception_handler(RequestValidationError, h.on_validation_error)
    app.add_exception_handler(HTTPException, h.on_http_error)
    app.add_exception_handler(Exception, h.on_system_error)
