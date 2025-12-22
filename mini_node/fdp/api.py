from typing import Annotated

from fastapi import APIRouter, Path, Request
from rdflib import Graph
from fastapi.responses import Response, PlainTextResponse
from starlette.status import HTTP_404_NOT_FOUND

import mini_node.fdp.service as fdp

# Path parameter types:
CatalogIdType = Annotated[str, Path(min_length=1, max_length=40)]
DatasetIdType = Annotated[str, Path(min_length=1, max_length=40)]
ProfileIdType = Annotated[str, Path(min_length=1, max_length=40)]
ShaclIdType = Annotated[str, Path(min_length=1, max_length=40)]

not_found_response = PlainTextResponse(
    "Sorry, this URL path is not supported",
    status_code=HTTP_404_NOT_FOUND,
)

# Exported router with endpoints:
router = APIRouter(tags=["fair-data-point"], prefix=fdp.get_base_path())


def base_url(request: Request) -> str:
    """Returns base URL (without path) of the service."""
    return str(request.base_url)


def to_response(request: Request, graph: Graph | None) -> Response:
    """Converts the graph into RDF Turtle or JSON-LD response. Alternatively,
    when graph is missing, returns a blank HTTP 404 response."""

    if graph is None:
        return not_found_response

    media_type = "text/turtle"
    output_format = "turtle"
    if request.headers.get("Accept") == "application/ld+json":
        media_type = "application/ld+json"
        output_format = "json-ld"

    content = graph.serialize(format=output_format)
    return Response(media_type=media_type, content=content)


def as_plain_text(payload) -> Response:
    """Returns a text/plain response, if the payload is present. Alternatively,
    when payload is missing, returns a blank HTTP 404 response."""
    if payload is None:
        return not_found_response
    return PlainTextResponse(payload)


@router.get("")
def get_fairdp_info(request: Request) -> Response:
    fdp_info = fdp.get_service_info(base_url(request))
    return to_response(request, fdp_info)


@router.get("/valid")
def get_fairdp_info_report(request: Request) -> Response:
    fdp_info = fdp.get_service_info(base_url(request))
    url = str(request.url.replace(query=None))
    return as_plain_text(fdp.validate_graph(fdp_info, url))


@router.get("/catalog")
async def get_catalogs(request: Request) -> Response:
    catalogs = fdp.get_catalogs(base_url(request))
    return to_response(request, catalogs)


@router.get("/catalog/valid")
async def get_catalogs_report(request: Request) -> Response:
    catalogs = fdp.get_catalogs(base_url(request))
    url = str(request.url.replace(query=None))
    return as_plain_text(fdp.validate_graph(catalogs, url, "catalogs"))


@router.get("/catalog/{catalog_id}")
async def get_catalog(request: Request, catalog_id: CatalogIdType) -> Response:
    catalog = fdp.get_catalog(base_url(request), catalog_id)
    return to_response(request, catalog)


@router.get("/catalog/{catalog_id}/valid")
def get_catalog_report(request: Request, catalog_id: CatalogIdType) -> Response:
    catalog = fdp.get_catalog(base_url(request), catalog_id)
    url = str(request.url.replace(query=None))
    return as_plain_text(fdp.validate_graph(catalog, url))


@router.get("/dataset/{dataset_id}")
async def get_dataset(request: Request, dataset_id: DatasetIdType) -> Response:
    dataset = fdp.get_dataset(base_url(request), dataset_id)
    return to_response(request, dataset)


@router.get("/dataset/{dataset_id}/valid")
def get_dataset_report(request: Request, dataset_id: DatasetIdType) -> Response:
    dataset = fdp.get_dataset(base_url(request), dataset_id)
    url = str(request.url.replace(query=None))
    return as_plain_text(fdp.validate_graph(dataset, url))


@router.get("/profile/{profile_id}")
async def get_profile(request: Request, profile_id: ProfileIdType) -> Response:
    return to_response(request, fdp.get_profile(base_url(request), profile_id))


@router.get("/profile/{profile_id}/valid")
async def get_profile(request: Request, profile_id: ProfileIdType) -> Response:
    profile = fdp.get_profile(base_url(request), profile_id)
    url = str(request.url.replace(query=None))
    return as_plain_text(fdp.validate_graph(profile, url))


@router.get("/shacl/{shacl_id}")
async def get_shacl(request: Request, shacl_id: ShaclIdType) -> Response:
    resource_url = str(request.url.replace(query=None))
    return to_response(request, fdp.get_shacl(resource_url, shacl_id))
