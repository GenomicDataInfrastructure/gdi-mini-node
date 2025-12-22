from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints

SequenceString = Annotated[str, StringConstraints(pattern=r"^[ACGTUNRYSWKMBDHV.-]*$")]
CURIEString = Annotated[str, StringConstraints(pattern=r"^\w[^:]+:.+$")]


class Identifiers(BaseModel):
    genomicHGVSId: str


class Number(BaseModel):
    type: str = "Number"
    value: int


class SequenceInterval(BaseModel):
    type: str = "SequenceInterval"
    start: Number
    end: Number


class SequenceLocation(BaseModel):
    type: str = "SequenceLocation"
    sequence_id: CURIEString
    interval: SequenceInterval


class LegacyVariation(BaseModel):
    location: SequenceLocation
    referenceBases: SequenceString
    alternateBases: SequenceString
    variantType: str


# Based on this document:
# https://docs.google.com/document/d/1LLzp6zZT3fSM1XxOXHuRqwJje1v726Z2/edit
class PopulationFrequency(BaseModel):
    population: str
    alleleFrequency: float
    alleleCount: int | None
    alleleCountHomozygous: int | None
    alleleCountHeterozygous: int | None
    alleleCountHemizygous: int | None
    alleleNumber: int | None


# Based on this document:
# https://docs.google.com/document/d/1LLzp6zZT3fSM1XxOXHuRqwJje1v726Z2/edit
class FrequencyInPopulations(BaseModel):
    source: str = "The Genome of Europe"
    sourceReference: str = "https://genomeofeurope.eu/"
    numberOfPopulations: int
    populations: Annotated[list[PopulationFrequency], Field(min_length=1)]


class AlleleFreqResult(BaseModel):
    identifiers: Identifiers | None = None
    variantInternalId: str
    variation: LegacyVariation
    frequencyInPopulations: list[FrequencyInPopulations]
