from pydantic import BaseModel, Field, HttpUrl

from mini_node.beacon.model.enums import EntityType


class Endpoint(BaseModel):
    entryType: EntityType
    openAPIEndpointsDefinition: HttpUrl | None = None
    rootUrl: HttpUrl
    singleEntryUrl: str | None = None


class BeaconMap(BaseModel):
    schemaUrl: str = Field(..., serialization_alias="$schema")
    endpointSets: dict[EntityType, Endpoint]
