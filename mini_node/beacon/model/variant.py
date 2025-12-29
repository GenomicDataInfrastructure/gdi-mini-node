from fastapi_cloud_cli.utils.pydantic_compat import model_dump
from typing_extensions import Annotated

from pydantic import (
    BaseModel,
    Field,
    NonNegativeInt,
    PositiveInt,
    StringConstraints,
    field_validator,
)

from mini_node.data.registry import BeaconAssembly

SequenceString = Annotated[
    str, StringConstraints(pattern=r"^[ACGTUNRYSWKMBDHV.-]*$")]

PositionRange = Annotated[
    list[NonNegativeInt], Field(min_length=1, max_length=2)]


class VariantQueryParameters(BaseModel):
    assemblyId: str | None = None
    geneId: str | None = None
    mateName: str | None = None
    aminoacidChange: str | None = None
    genomicAlleleShortForm: str | None = None
    referenceName: str | None = None
    referenceBases: SequenceString | None = None
    alternateBases: SequenceString | None = None
    start: PositionRange | str | None = None
    end: PositionRange | str | None = None
    variantType: str | None = None
    variantMinLength: NonNegativeInt | None = None
    variantMaxLength: PositiveInt | None = None

    @field_validator("start", "end", mode="before")
    @classmethod
    def split_range_nums(cls, value: str) -> object:
        if isinstance(value, str):
            try:
                return [int(v) for v in value.split(",") if v.strip()]
            except ValueError:
                raise ValueError(
                    "expected one or two (comma-separated) non-negative integers"
                )
        return value

    def has_values(self) -> bool:
        return len(self.model_dump(exclude_none=True)) > 0

    def has_unsupported_values(self) -> bool:
        return self.geneId is not None \
            or self.mateName is not None \
            or self.aminoacidChange is not None \
            or self.genomicAlleleShortForm is not None \
            or self.alternateBases is not None \
            or self.variantMinLength is not None \
            or self.variantMaxLength is not None

    def has_sufficient_values(self) -> bool:
        return self.assemblyId is not None \
            and self.assemblyId in BeaconAssembly \
            and self.referenceName is not None \
            and self.referenceBases is not None \
            and self.alternateBases is not None \
            and self.start is not None

    def is_not_sufficient(self) -> bool:
        return not self.has_unsupported_values() and self.has_sufficient_values()
