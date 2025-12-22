import logging

from fastapi.encoders import jsonable_encoder

from ..model.common import BeaconRequest
from ..model.dataset import BeaconDataset
from ...data import DATA

"""Implementation for exposing Beacon datasets (only for aggregated Beacon).

Here is the standard endpoint description:
https://github.com/ga4gh-beacon/beacon-v2/blob/v2.2.0/models/src/beacon-v2-default-model/datasets/endpoints.yaml

Here, request parameters "requestedSchema" and "filters" are ignored.
Only "skip" and "limit" are supported.
"""

_log = logging.getLogger(__name__)


def get_datasets(request: BeaconRequest) -> list[BeaconDataset]:
    """Returns those dataset records that are visible in the aggregated Beacon.
    Values for dataset properties are the same as in the FAIR Data Point API.
    """

    dataset_ids = DATA.aggregated_beacon.get_dataset_ids()

    page = request.query.pagination
    limit = page.limit if page and page.limit is not None else 10
    skip = page.skip if page and page.skip is not None else 0

    results = []

    if skip >= len(dataset_ids):
        return results
    elif skip > 0:
        dataset_ids = dataset_ids[skip:]

    for dataset_id in dataset_ids:
        props = DATA.fdp.datasets.get(dataset_id)
        if props is None:
            continue

        dataset = BeaconDataset(
            id=dataset_id,
            name=props.title,
            description=props.description,
            createDateTime=props.since,
            updateDateTime=props.updated,
        )

        results.append(jsonable_encoder(dataset))

        if len(results) >= limit:
            break

    return results


__all__ = ["get_datasets"]
