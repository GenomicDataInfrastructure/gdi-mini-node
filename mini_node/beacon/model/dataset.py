from datetime import datetime

from pydantic import BaseModel


# Based on this document:
# https://docs.genomebeacons.org/schemas-md/datasets_defaultSchema/
class BeaconDataset(BaseModel):
    id: str
    name: str
    description: str | None = None
    createDateTime: datetime | None = None
    updateDateTime: datetime | None = None

    # Omitted:
    # version: str | None
    # externalUrl: str | None
    # dataUseConditions: DUODataUse(id,label?,version,modifiers?) | None
    # info: dict | None
