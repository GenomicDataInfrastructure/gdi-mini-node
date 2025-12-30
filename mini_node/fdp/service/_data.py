from datetime import datetime
from typing import Any

from mini_node.data import DATA, fdp_config


def get_last_modified() -> datetime | None:
    """Retrieves the latest dataset state update time for all catalogs."""
    max_modified = fdp_config.since
    for dataset in DATA.fdp.datasets.values():
        if dataset.updated > max_modified:
            max_modified = dataset.updated
    return max_modified


def get_catalog_ids() -> list[str]:
    """Provides catalog IDs for generating URLS. This does not call the database
    as catalogs are configured in the configuration file (YAML)."""
    return list(sorted(DATA.fdp.catalogs.keys()))


def get_catalog_info(catalog_id: str) -> dict[str, Any] | None:
    """Retrieves the latest dataset state update time for a catalog ID."""
    catalog = DATA.fdp.catalogs.get(catalog_id)
    if catalog is None:
        return None

    dataset_ids = DATA.fdp.catalog_datasets.get(catalog_id, [])
    latest_update = catalog.since or fdp_config.since

    for dataset_id in dataset_ids:
        dataset = DATA.fdp.datasets.get(dataset_id)
        if latest_update is None or dataset.updated > latest_update:
            latest_update = dataset.updated

    return {
        "id": catalog_id,
        "title": catalog.title,
        "description": catalog.description,
        "since": catalog.since,
        "updated": latest_update,
        "dataset_ids": dataset_ids,
    }


def get_dataset_info(dataset_id: str) -> dict[str, str] | None:
    """Retrieves dataset properties in a dictionary, or None when not found."""
    dataset = DATA.fdp.datasets.get(dataset_id)

    if dataset is None or dataset.catalog_id not in DATA.fdp.catalogs:
        return None

    return {
        "id": dataset_id,
        "title": dataset.title,
        "description": dataset.description,
        "keywords": dataset.keywords,
        "since": dataset.since,
        "updated": dataset.updated,
        "min_age": dataset.min_age,
        "max_age": dataset.max_age,
        "record_count": dataset.record_count,
        "individual_count": dataset.individual_count,
        "data_provider_name": dataset.data_provider_name,
    }
