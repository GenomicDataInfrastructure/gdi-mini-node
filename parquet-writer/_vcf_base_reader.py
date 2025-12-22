import logging
from time import time
from typing import Any

from cyvcf2 import VCF, Variant

from _parquet import ParquetVcfWriter

_log = logging.getLogger(__name__)


class VcfBaseReader:
    """Base class for reading a VCF file."""

    def __init__(self, vcf_file: str):
        try:
            self._variant_row = 0  # Essentially for tracking the row number
            self._vcf_name = vcf_file
            self._vcf = VCF(vcf_file)
        except Exception as e:
            _log.debug("Failed to open VCF %s: %s", vcf_file, e,
                       exc_info=True)
            raise RuntimeError(
                f"Failed to open VCF file {vcf_file}: {e}") from e

    def samples(self) -> list[str]:
        """Returns a list of sample names."""
        return list(self._vcf.samples)

    def write_to(self, writer: ParquetVcfWriter):
        """Processes the variants in the VCF file and writes rows to the given
        Parquet writer."""

        start_file = time()
        iterator = iter(self._vcf)
        parquet_row_data = {}

        _log.info("VCF header parsed successfully. Starting to read variants.")

        while True:
            try:
                variant = next(iterator)
                self._variant_row += 1
            except StopIteration:
                break
            except Exception as e:
                self._fail(f"VCF parse error: {e}")
                return

            try:
                chr_id = self._get_valid_chr_id(variant)
                pos = self._get_valid_pos(variant)
                ref = self._get_valid_ref(variant)
            except RuntimeError:
                raise
            except Exception as e:
                self._fail(f"Invalid variant: {e}")
                return

            writer.at_chr_pos(chr_id, pos)

            parquet_row_data["POS"] = pos
            parquet_row_data["REF"] = ref

            if not self._before_alts(variant):
                # Skip this variant at all, as indicated by the implementation.
                continue

            # Gather Parquet rows for each ALT and each POPULATION value:
            for alt_index, alt in enumerate(variant.ALT):
                alt = alt.strip().upper()

                if not self._is_supported_alt(alt):
                    # _log.debug(
                    #    "Skipping ALT '%s' at variant row %d.",
                    #    alt, self._variant_row,
                    # )
                    continue

                parquet_row_data["ALT"] = alt
                parquet_row_data["VT"] = self._get_vt(ref, alt)

                if self._on_alt(variant, alt, alt_index, parquet_row_data):
                    # Add only when approved by the implementation.
                    writer.add_row(parquet_row_data)

        _log.debug("All VCF rows processed.")
        writer.close()

        _log.info("Processed VCF file %s in %.2f seconds.",
                  self._vcf_name, time() - start_file)
        _log.info("Produced %d Parquet files with a total of %d rows.",
                  len(writer.parquet_files), writer.total_row_count)

    def close(self):
        self._vcf.close()

    def _before_alts(self, variant: Variant) -> bool:
        # For custom implementations.
        return True

    def _on_alt(
            self, variant: Variant, alt: str, alt_index: int,
            parquet_row_data: dict[str, Any],
    ) -> bool:
        # For custom implementations.
        return True

    def _fail(self, message: str):
        message = self._vcf_name + f" [{self._variant_row}]: " + message
        _log.error(message)
        raise RuntimeError("VCF to Parquet process was halted due to a problem")

    def _get_valid_chr_id(self, variant: Variant) -> str:
        """Returns a normalized chromosome label.

        Removes "chr" prefix, replaces "MT" with "M", and if it's a letter,
        makes sure it's in uppercase.

        Invalid values raise a runtime error.
        """

        chr_id = variant.CHROM.strip()

        if chr_id.lower().startswith("chr"):
            norm = chr_id[3:]
        else:
            norm = chr_id
        norm = norm.upper()

        if norm == "MT":
            norm = "M"

        if norm in ("X", "Y", "M"):
            return norm

        try:
            if 1 <= int(norm) <= 22:
                return norm
        except:
            pass

        self._fail(f"Invalid chromosome label '{variant.CHROM}'")
        return ""  # This line is not reached.

    def _get_valid_pos(self, variant: Variant) -> int:
        try:
            pos = int(variant.POS) - 1
            if pos >= 0:
                return pos
        except:
            pass

        self._fail(f"Invalid POS '{variant.POS}'")
        return 0  # This line is not reached.

    def _get_valid_ref(self, variant: Variant) -> str:
        ref = variant.REF.strip().upper()
        if ref and all(base in "ACGTN" for base in ref):
            return ref

        self._fail(f"Invalid REF '{variant.REF}'")
        return ""  # This line is not reached.

    # Might take algorithm from vt: https://genome.sph.umich.edu/wiki/Variant_classification
    # Could also read the variant type from VCF, if present: `variant.INFO.get("VT")`
    def _get_vt(self, ref: str, alt: str) -> str:
        # The most common case first:
        if len(ref) == 1 and len(alt) == 1 and ref != alt:
            return "SNP"

        trimmed_ref, trimmed_alt = self._trim_alleles(ref, alt)
        len_ref = len(trimmed_ref)
        len_alt = len(trimmed_alt)

        if len_ref == 1 and len_alt == 1 and trimmed_ref != trimmed_alt:
            return "SNP"
        elif len_ref != len_alt:
            return "INDEL"
        else:
            return "UNKNOWN"

    def _is_supported_alt(self, alt: str) -> bool:
        if alt is None or alt == "":
            self._fail(f"Empty ALT")

        if alt.startswith("<") and alt.endswith(">"):
            return False  # Symbolic allele

        if "[" in alt or "]" in alt:
            return False  # Breakend notation

        if alt == ".":
            return False  # Missing value, i.e. no alt called

        if alt.startswith(".") or alt.endswith("."):
            return False  # Single breakend

        if alt == "*":
            return False  # Allele missing due to overlapping deletion

        if all(base in "ACGTN" for base in alt):
            return True

        self._fail(f"Invalid ALT '{alt}'")
        return False

    @staticmethod
    def _trim_alleles(ref: str, alt: str) -> tuple[str, str]:
        # Trim common prefix
        while ref and alt and ref[0] == alt[0]:
            ref = ref[1:]
            alt = alt[1:]

        # Trim common suffix
        while ref and alt and ref[-1] == alt[-1]:
            ref = ref[:-1]
            alt = alt[:-1]

        return ref, alt
