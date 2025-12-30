from enum import StrEnum

from pydantic import BaseModel, Field

from .entry_types import EntryType
from ..enums import Granularity


class SecurityLevel(StrEnum):
    PUBLIC = "PUBLIC"
    REGISTERED = "REGISTERED"
    CONTROLLED = "CONTROLLED"


class ProductionStatus(StrEnum):
    DEV = "DEV"
    TEST = "TEST"
    PROD = "PROD"


class MaturityAttributes(BaseModel):
    productionStatus: ProductionStatus


class SecurityAttributes(BaseModel):
    defaultGranularity: Granularity | None = None
    securityLevels: list[SecurityLevel] | None = None


class BeaconConfiguration(BaseModel):
    schemaUrl: str = Field(None, serialization_alias="$schema")
    entryTypes: dict[str, EntryType]
    maturityAttributes: MaturityAttributes
    securityAttributes: SecurityAttributes
