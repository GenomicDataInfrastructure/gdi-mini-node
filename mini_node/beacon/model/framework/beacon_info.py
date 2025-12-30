from datetime import datetime
from typing import Annotated

from pydantic import AnyUrl, BaseModel, HttpUrl, UrlConstraints

from . import BeaconEnvironment

RestrictedSchemes = UrlConstraints(allowed_schemes=["http", "https", "mailto"])


class BeaconInfoOrganization(BaseModel):
    id: str
    name: str
    description: str | None = None
    address: str | None = None
    welcomeUrl: HttpUrl | None = None
    contactUrl: Annotated[AnyUrl, RestrictedSchemes] | None = None
    logoUrl: HttpUrl | None = None


class BeaconInfo(BaseModel):
    id: str
    name: str
    apiVersion: str
    environment: BeaconEnvironment
    organization: BeaconInfoOrganization
    description: str | None = None
    version: str | None = None
    welcomeUrl: HttpUrl | None = None
    alternativeUrl: HttpUrl | None = None
    createDateTime: datetime | None = None
    updateDateTime: datetime | None = None
    info: dict | None = None
