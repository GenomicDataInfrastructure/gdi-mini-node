from pyshacl import validate
from rdflib import Graph, DCTERMS

from ._data import (
    get_catalog_ids,
    get_catalog_info,
    get_dataset_info,
    get_last_modified,
)
from ._loader import load_tmpl_config

fdp_tmpl_config = load_tmpl_config()

def get_base_path() -> str:
    return fdp_tmpl_config.generator.base_path()

def get_service_info(base_url: str) -> Graph:
    params = {
        "catalogs": _id_to_url(base_url, "catalog", get_catalog_ids()),
        "updated": get_last_modified(),
    }
    g = fdp_tmpl_config.render("fairdp", base_url, params)
    return fdp_tmpl_config.render("catalogs", base_url, params, g)


def get_profile(base_url: str, profile_id: str) -> Graph | None:
    if profile_id not in fdp_tmpl_config.shacls:
        return None
    shacl_url = fdp_tmpl_config.base_url(base_url, item_id=f"shacl/{profile_id}")
    params = {"id": profile_id, "shacl_url": shacl_url}
    return fdp_tmpl_config.render("profile", base_url, params)


def get_shacl(resource_url: str, shacl_id: str) -> str | None:
    return fdp_tmpl_config.shacl(resource_url, shacl_id)


def get_catalogs(base_url: str) -> Graph:
    catalog_urls = _id_to_url(base_url, "catalog", get_catalog_ids())
    params = {"catalogs": catalog_urls}
    return fdp_tmpl_config.render("catalogs", base_url, params)


def get_catalog(base_url: str, catalog_id: str) -> Graph | None:
    catalog_params = get_catalog_info(catalog_id)
    if catalog_params is None:
        return None

    dataset_ids = catalog_params["dataset_ids"]
    catalog_params["datasets"] = _id_to_url(base_url, "dataset", dataset_ids)
    return fdp_tmpl_config.render("catalog", base_url, catalog_params)


def get_dataset(base_url: str, dataset_id: str) -> Graph | None:
    dataset_params = get_dataset_info(dataset_id)
    if dataset_params is None:
        return None
    return fdp_tmpl_config.render("dataset", base_url, dataset_params)


def validate_graph(graph: Graph, request_url: str, shacl_id=None) -> str | None:
    if graph is None:
        return None

    if shacl_id is None:
        for o in graph.objects(None, DCTERMS.conformsTo):
            shacl_id = str(o)
            shacl_id = shacl_id[shacl_id.rindex("/") + 1 :]
            break
        else:
            return "dct:conformsTo was not found in the graph"

    shacl_graph = get_shacl(request_url, shacl_id)
    _, _, v_text = validate(graph, shacl_graph=shacl_graph, allow_warnings=True)
    return v_text


def _to_base_url(base_url: str, tmpl_id: str) -> str:
    return fdp_tmpl_config.base_url(base_url, tmpl_id).rstrip("/") + "/"


def _id_to_url(base_url: str, tmpl_id: str, ids: list[str] | set[str]) -> list[str]:
    if len(ids) == 0:
        return ids
    item_base_url = _to_base_url(base_url, tmpl_id)
    return [item_base_url + item_id for item_id in ids]
