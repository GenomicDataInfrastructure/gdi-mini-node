from fastapi import FastAPI

def add_endpoints(app: FastAPI):
    from ..data import fdp_config
    if fdp_config is not None:
        from .api import router
        app.include_router(router)
