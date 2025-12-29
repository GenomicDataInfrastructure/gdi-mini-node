import logging
from datetime import datetime, UTC

from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse, JSONResponse

from .data import DATA
from .data.registry import BeaconData
from .setup import app_version, info_page_credentials

log = logging.getLogger(__name__)

app_instance: FastAPI | None = None


def add_endpoints(app: FastAPI):
    app.add_route("/", get_status, ["GET"], "status")
    app.add_route("/health", get_health, ["GET"], "health")
    global app_instance
    app_instance = app


async def get_status(request: Request) -> PlainTextResponse:
    response = _check_access(request)
    if response:
        return response

    global app_instance
    base_url = str(request.base_url).rstrip("/")

    content = """GDI Node Deployment
===================
"""
    if len(DATA.problematic_files) > 0:
        content += """
Problematic Files
-----------------

"""
        for file_path, issue in DATA.problematic_files.items():
            content += f"* [{file_path}]\n  {issue}\n"
        content += "\n"

    content += """
Endpoints
---------

"""
    paths = []
    for route in app_instance.routes:
        paths.append((list(route.methods - {"HEAD"})[0], route.path))

    for path in sorted(paths, key=lambda x: x[1]):
        content += f"* {path[0]:4} {base_url}{path[1]}\n"

    content += """


FAIR Data Point
===============

Catalogs
--------

"""
    for key, catalog in DATA.fdp.catalogs.items():
        content += f"* [{key}] {catalog.title}\n"
        for dataset_id in DATA.fdp.catalog_datasets.get(key, []):
            dataset = DATA.fdp.datasets[dataset_id]
            content += f"  - [{dataset_id}] {dataset.title}\n"

    hidden_catalogs: dict[str, list[tuple[str, str]]] = {}
    catalog_ids = DATA.fdp.catalogs.keys()
    for dataset_id, props in DATA.fdp.datasets.items():
        if props.catalog_id not in catalog_ids:
            if props.catalog_id not in hidden_catalogs:
                hidden_catalogs[props.catalog_id] = []
            hidden_catalogs[props.catalog_id].append((dataset_id, props.title))

    if len(hidden_catalogs) > 0:
        content += ("\n\nHidden Datasets (bad catalog_id value)"
                    "\n--------------------------------------\n\n")
        for catalog_id, datasets in hidden_catalogs.items():
            content += f"* [{catalog_id}]\n"
            for dataset_id, dataset_title in datasets:
                content += f"  - [{dataset_id}] {dataset_title}\n"

    content += _beacon_data_as_str(DATA.aggregated_beacon, "Aggregated")
    content += _beacon_data_as_str(DATA.sensitive_beacon, "Sensitive")

    return PlainTextResponse(content)


def get_health(_: Request) -> JSONResponse:
    ts = datetime.now(tz=UTC).replace(microsecond=0).isoformat()
    ts = ts.replace("+00:00", "Z")
    return JSONResponse({
        "timestamp": ts, "version": app_version, "healthy": True,
    })


def _check_access(request: Request) -> PlainTextResponse | None:
    if info_page_credentials is None:
        return None

    auth_value = request.headers.get("Authorization")
    if auth_value == info_page_credentials:
        return None

    headers = {"WWW-Authenticate": "Basic"}
    msg = "This resource requires BASIC authentication."
    return PlainTextResponse(msg, status_code=401, headers=headers)


def _beacon_data_as_str(data: BeaconData, title: str) -> str:
    title = f"GA4GH Beacon ({title})"
    content = "\n\n\n" + title + "\n" + "=" * len(title) + "\n\n"
    for assembly, datasets in data.assemblies.items():
        content += f"{assembly}\n{'-' * len(assembly)}\n\n"
        for dataset in datasets:
            content += f"* [{dataset.dataset_id}]\n"
            if dataset.individuals_parquet:
                content += f"  - [{dataset.individuals_parquet}]\n"
            for key in sorted(dataset.chr_group_files.keys(),
                              key=_file_sort_key):
                content += f"  - [{dataset.chr_group_files[key]}]\n"

    return content


def _file_sort_key(chr_group: str) -> int:
    middle = chr_group.index(".")
    key = int(chr_group[middle + 1:]) if middle > 0 else 0
    chromosome = chr_group[0:middle] if middle > 0 else chr_group
    if chromosome == "X":
        return key + 25000
    elif chromosome == "Y":
        return key + 26000
    elif chromosome == "M":
        return key + 27000
    else:
        return key + int(chromosome) * 1000


__all__ = ["add_endpoints"]
