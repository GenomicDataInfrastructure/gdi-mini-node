from typing import Annotated

from pydantic import BaseModel, StringConstraints

CURIEString = Annotated[str, StringConstraints(pattern=r"^\w[^:]+:.+$")]


class ReferenceToSchema(BaseModel):
    id: str
    name: str
    description: str | None = None
    referenceToSchemaDefinition: str


class OntologyTerm(BaseModel):
    id: CURIEString
    label: str | None = None


class EntryType(BaseModel):
    id: str
    name: str
    description: str | None = None
    partOfSpecification: str
    defaultSchema: ReferenceToSchema
    ontologyTermForThisType: OntologyTerm


class EntryTypes(BaseModel):
    entryTypes: dict[str, EntryType]
