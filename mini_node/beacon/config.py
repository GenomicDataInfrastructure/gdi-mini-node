from datetime import datetime

from pydantic import AnyUrl, BaseModel, HttpUrl, PositiveInt, UrlConstraints, \
    ConfigDict
from typing_extensions import Annotated

from .model.enums import Granularity
from .model.framework import BeaconEnvironment
from .model.framework.configuration import ProductionStatus, SecurityLevel

UrlOrEmail = Annotated[
    AnyUrl, UrlConstraints(allowed_schemes=["http", "https", "mailto"])]


class BeaconInfoConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id: str
    name: str
    version: str
    environment: BeaconEnvironment
    description: str | None = None
    alternativeUrl: HttpUrl | None = None
    documentationUrl: str | None = None
    createdAt: datetime | None = None
    updatedAt: datetime | None = None
    info: dict | None = None


class BeaconOrganisationConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id: str
    name: str
    description: str | None = None
    address: str | None = None
    welcomeUrl: HttpUrl
    contactUrl: UrlOrEmail | None = None
    logoUrl: str | None = None


class BeaconOidcConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')
    issuer: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    required_visas: list[dict[str, str]] | None = None

    def is_effective(self) -> bool:
        return self.issuer is not None and \
            self.client_id is not None and \
            self.client_secret is not None

class BeaconBasicAuthConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')
    username: str | None = None
    password: str | None = None

    def is_effective(self) -> bool:
        return self.username is not None and self.password is not None

class BeaconConfigurationConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')
    productionStatus: ProductionStatus
    securityLevel: SecurityLevel
    granularity: Granularity
    hideLowerCounts: PositiveInt = 1
    oidc: BeaconOidcConfig | None = None
    basic: list[BeaconBasicAuthConfig] | None = None


class BeaconSchemaConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id: str
    name: str
    description: str
    path: str


class BeaconComplianceConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')
    specification: str
    schemaBaseUrl: str
    schemaVersionTag: str
    schemas: list[BeaconSchemaConfig]

    def url(self, path: str) -> str:
        return self.schemaBaseUrl + self.schemaVersionTag + path

    def get_schema(self, entity_id: str) -> BeaconSchemaConfig:
        for schema in self.schemas:
            if schema.id == entity_id:
                return schema
        raise RuntimeError(f"[configuration] schema [{entity_id}] not found")

    def get_schema_url(self, entity_id: str) -> str:
        schema = self.get_schema(entity_id)
        return self.url(schema.path)


class BeaconOntologyTermConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id: str
    label: str


class BeaconEntryTypesConfig(BaseModel):
    model_config = ConfigDict(extra='forbid')
    id: str
    name: str
    description: str
    openapi: str
    schemaId: str
    mainPath: str
    itemPath: str | None = None
    ontologyTerm: BeaconOntologyTermConfig


class BeaconConfig(BaseModel):
    """Metadata model for beacon-aggregated.yaml and beacon-sensitive.yaml."""
    model_config = ConfigDict(extra='forbid')
    base_path: str
    info: BeaconInfoConfig
    configuration: BeaconConfigurationConfig


class BeaconCommonConfig(BaseModel):
    """Metadata model for beacon-common.yaml."""
    model_config = ConfigDict(extra='forbid')
    organisation: BeaconOrganisationConfig
    compliance: BeaconComplianceConfig
    entryTypes: list[BeaconEntryTypesConfig]


class BeaconContext(BaseModel):
    """Combines common and service-specific Beacon configuration."""
    model_config = ConfigDict(extra='forbid')
    common: BeaconCommonConfig
    service: BeaconConfig
    aggregated: bool

    def __init__(self, common: BeaconCommonConfig, service: BeaconConfig, aggregated: bool):
        super().__init__(common=common, service=service, aggregated=aggregated)
