from pydantic import BaseModel, Field


class Resource(BaseModel):
    id: str
    name: str | None = None
    url: str | None = None
    version: str | None = None
    nameSpacePrefix: str | None = None
    iriPrefix: str | None = None


class FilteringTermInResponse(BaseModel):
    type: str
    id: str
    label: str | None = None
    scopes: list[str] | None = None
    values: list[str] | None = Field(None, min_length=1)


class FilteringTerms(BaseModel):
    resources: list[Resource] | None = None
    filteringTerms: list[FilteringTermInResponse] | None = None
