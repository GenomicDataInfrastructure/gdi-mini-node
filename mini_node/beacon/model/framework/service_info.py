from datetime import datetime
from typing import Annotated

from pydantic import AnyUrl, BaseModel, HttpUrl, UrlConstraints

from . import BeaconEnvironment

RestrictedSchemes = UrlConstraints(allowed_schemes=["http", "https", "mailto"])


class ServiceInfoType(BaseModel):
    artifact: str
    group: str
    version: str


class BeaconServiceInfoOrganization(BaseModel):
    name: str
    url: HttpUrl


class ServiceInfo(BaseModel):
    id: str
    name: str
    type: ServiceInfoType
    description: str | None = None
    organization: BeaconServiceInfoOrganization
    contactUrl: Annotated[AnyUrl, RestrictedSchemes] | None = None
    documentationUrl: HttpUrl | None = None
    createdAt: datetime | None = None
    updatedAt: datetime | None = None
    environment: BeaconEnvironment | None = None
    version: str
