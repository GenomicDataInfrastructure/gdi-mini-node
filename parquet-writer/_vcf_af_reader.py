import logging
from typing import Any

from cyvcf2 import Variant

from _vcf_base_reader import VcfBaseReader

_log = logging.getLogger(__name__)


class PopulationFields:
    """Helper class mapping population-fields to standard INFO fields, also
    for storing field-values and mapping them to the Parquet row."""

    # These are the supported (standard) INFO fields. Only AF is mandatory.
    STANDARD_FIELDS = {"AF", "AC", "AC_Het", "AC_Hom", "AC_Hemi", "AN"}

    def __init__(self) -> None:
        self._actual_to_standard_fields: dict[str, str] = {}
        self._values_of_standard_fields: dict[str, Any] = {}

    def add_field(self, actual_field: str, standard_field: str) -> None:
        self._actual_to_standard_fields[actual_field] = standard_field

    def actual_fields(self):
        return self._actual_to_standard_fields.keys()

    def set_field_value(self, actual_field: str, value) -> None:
        standard_field = self._actual_to_standard_fields[actual_field]
        self._values_of_standard_fields[standard_field] = value

    def has_af_field(self):
        for value in self._actual_to_standard_fields.values():
            if value == "AF":
                return True
        return False

    def to_parquet_row(self, parquet_row: dict, alt_index: int):
        for std_field in self.STANDARD_FIELDS:
            value = self._get_value(std_field, alt_index)
            parquet_row[std_field.upper()] = value

    def _get_value(self, std_field: str, index: int):
        items = self._values_of_standard_fields.get(std_field)
        if items is None or std_field == "AN" and type(items) in (int, float):
            return items
        return items[index]

    def __repr__(self):
        result = ""
        for actual, std in self._actual_to_standard_fields.items():
            if result != "":
                result += ", "
            result += std + "=" + actual
        return result


class VcfAlleleFreqReader(VcfBaseReader):
    """Helper class reading allele frequency from a VCF file."""

    def __init__(self, vcf_file: str, population: str | None):
        super().__init__(vcf_file)

        _log.info("Processing VCF [%s] for allele frequency data.", vcf_file)

        self._an_scalar: bool | None = None  # Whether AN is scalar or array

        # Gather info about the INFO fields per population from the VCF header:
        self._population_fields: dict[str, PopulationFields] = {}
        self._extract_info_fields_from_header(population)

    def _before_alts(self, variant: Variant) -> bool:
        # Gather AF values before processing one or more ALT values:
        for _, fields in self._population_fields.items():
            for info_field in fields.actual_fields():
                value = self._get_valid_freq_value(variant, info_field)
                fields.set_field_value(info_field, value)
        return True

    def _on_alt(
            self, variant: Variant, alt: str, alt_index: int,
            parquet_row_data: dict[str, Any],
    ) -> bool:
        for population, values in self._population_fields.items():
            parquet_row_data["POPULATION"] = population
            values.to_parquet_row(parquet_row_data, alt_index)
        return True

    def close(self):
        self._vcf.close()

    def _extract_info_fields_from_header(
            self, default_population: str,
    ) -> None:
        population_field_prefixes = {"AF_", "AC_", "AN_"}
        sex_values = {"M", "F"}

        default_population_fields = PopulationFields()

        for field in self._vcf.header_iter():
            if field.type != "INFO":
                continue

            props = field.info()
            field_id = props.get("ID", "")
            field_number = props.get("Number")

            if field_id in PopulationFields.STANDARD_FIELDS:
                self._validate_field(field_id, field_number)
                default_population_fields.add_field(field_id, field_id)
                continue

            if field_id[:3] not in population_field_prefixes:
                continue

            # Try to extract population, which is one of following:
            # 1. 2-letter country-code (e.g. "AB")
            # 2. sex ("M" or "F")
            # 3. both (e.g. "AB_M")
            #
            # There is only one item before underscore+population (e.g. "AC").
            # Population might be followed by some additional text
            # (e.g. "AC_AB_M_Het").

            parts = field_id.split("_")
            part1 = parts[1]
            part2 = parts[2] if len(parts) > 1 else ""

            if part1 in sex_values:
                population_code = part1
                normalised_field_id = "_".join([parts[0], *parts[2:]])
            elif len(part1) == 2 and part1 == part1.upper():
                if part2 in sex_values:
                    part1 += "_" + part2
                    normalised_field_id = "_".join([parts[0], *parts[3:]])
                else:
                    normalised_field_id = "_".join([parts[0], *parts[2:]])
                population_code = part1
            else:
                _log.info("Ignoring INFO field [%s] as the population info "
                          "could not be extracted.", field_id)
                continue

            if normalised_field_id not in PopulationFields.STANDARD_FIELDS:
                _log.warning(
                    "Ignoring INFO field [%s] as the normalised field [%s] is "
                    "not supported (case-sensitive check).",
                    field_id, normalised_field_id)
                continue

            self._validate_field(field_id, field_number)

            if population_code in self._population_fields:
                fields = self._population_fields[population_code]
            else:
                _log.info("Extracted population [%s].", population_code)
                fields = PopulationFields()
                self._population_fields[population_code] = fields

            fields.add_field(field_id, normalised_field_id)

        if len(self._population_fields) == 0:
            if default_population is None:
                raise RuntimeError(
                    "The VCF does not contain GDI populations. Please provide "
                    "the population label (country code) via -c flag.")
            self._population_fields[default_population] = \
                default_population_fields

        _log.info("Found %d population(s) using INFO fields:",
                  len(self._population_fields))
        for population_code, fields in self._population_fields.items():
            _log.info("[%s]: %s", population_code, repr(fields))
            assert fields.has_af_field(), "AF field was not found?"

    def _validate_field(self, field_id: str, field_number):
        if field_id == "AN" or field_id.startswith("AN_"):
            an_scalar = field_number is None or field_number == "1"

            if self._an_scalar is not None and self._an_scalar != an_scalar:
                raise RuntimeError(
                    f"INFO field [{field_id}] has a different Number value. "
                    f"Use same Number for all AN* fields."
                )

            # Remembers if AN is scalar value:
            self._an_scalar = an_scalar

            # Exit on scalar type to avoid the upcoming Number!=A check.
            if self._an_scalar:
                return

        # Now other cases: AF, AC, AC_Hom, AC_Het, AC_Hemi:
        if field_number != "A":
            raise RuntimeError(
                f"{field_id} must have Number=A (one value per ALT allele), "
                f"but is Number={field_number}")

    def _get_valid_freq_value(
            self, variant: Variant, freq_field: str,
    ) -> list[float | int] | float | int | None:
        value = variant.INFO.get(freq_field)
        if value is None:
            return None

        if freq_field == "AN" and self._an_scalar:
            if not isinstance(value, (int, float)) or value < 0:
                self._fail(f"AN must be a non-negative number, but is {value}")
            return value
            # If not scalar, run the following checks on the AN value.

        if isinstance(value, tuple) or isinstance(value, list):
            if len(value) != len(variant.ALT):
                self._fail(
                    f"{freq_field} value {value} must have the same length "
                    f"{len(variant.ALT)} as ALT")
            value = list(value)
        else:
            if len(variant.ALT) > 1:
                self._fail(
                    f"{freq_field} value {value} must be a list of length "
                    f"{len(variant.ALT)}")
            value = [value]

        if not all(isinstance(f, (int, float)) for f in value) or \
                not all(f >= 0 for f in value):
            self._fail(
                f"{freq_field} value {value} must be a list of non-negative "
                f"numbers")

        return value
