from datetime import date, datetime

from pydantic import BaseModel, EmailStr, HttpUrl

"""Data model of the fdp.yaml configuration."""


class FdpContactPoint(BaseModel):
    data_access_body: str
    name: str | None = None
    homepage: str | None = None
    email: EmailStr | None


class FdpCatalog(BaseModel):
    title: str
    description: str
    since: date | datetime | None = None


class FdpConfig(BaseModel):
    title: str
    description: str | None = None
    theme: HttpUrl | None = None
    theme_taxonomy: HttpUrl | None = None
    language: HttpUrl | None = None
    legislation: HttpUrl
    license: HttpUrl
    access_right: HttpUrl
    user_portal_datasets: HttpUrl | None = None
    since: date | datetime
    contact_point: FdpContactPoint
    keywords: list[str] | None = None
    catalogs: dict[str, FdpCatalog]


class FdpDataset(BaseModel):
    """Data model of metadata.yaml."""
    title: str
    description: str
    catalog_id: str
    keywords: list[str] | None = None
    since: datetime
    updated: datetime
    min_age: int | None = None
    max_age: int | None = None
    individual_count: int | None = None
    record_count: int
    data_provider_name: str
