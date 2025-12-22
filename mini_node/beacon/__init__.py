from fastapi import FastAPI

def add_endpoints(app: FastAPI):
    from ..data.config import beacon_aggregated, beacon_sensitive

    if beacon_aggregated is None and beacon_sensitive is None:
        return

    from .api.router import beacon_router

    if beacon_aggregated is not None:
        router = beacon_router(beacon_aggregated)
        app.include_router(router)

    if beacon_sensitive is not None:
        router = beacon_router(beacon_sensitive)
        app.include_router(router)
