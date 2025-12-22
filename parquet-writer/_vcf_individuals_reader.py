from logging import getLogger
from typing import Any

import numpy as np
from cyvcf2 import Variant

from _vcf_base_reader import VcfBaseReader

_log = getLogger(__name__)


class VcfIndividualsReader(VcfBaseReader):

    def __init__(self, vcf_file: str):
        super().__init__(vcf_file)
        self._genotype = None
        _log.info("Processing VCF [%s] for individuals data.", vcf_file)


    def _before_alts(self, variant: Variant) -> bool:
        self._genotype = self._get_genotype(variant)

        # Skip when all samples match to REF (0) or allele is unknown ('.', encoded as -1).
        if np.all((self._genotype == 0) | (self._genotype == -1)):
            _log.debug(
                "Skipping variant at POS=%d: all samples match to REF or "
                "allele is unknown", variant.POS)
            self._genotype = None
            return False

        return True

    def _on_alt(
            self,
            variant: Variant,
            alt: str,
            alt_index: int,
            parquet_row_data: dict[str, Any],
    ) -> bool:
        if self._genotype is None:
            return False

        # Array of samples with True/False as value if they have the ALT:
        # +1 as genotype=0 is REF, 1 is alt_index=0, 2 is alt_index=1, etc.
        arr_bools = np.any(self._genotype == alt_index + 1, axis=1)

        # Array of indices (of samples) for True values:
        sample_indices = np.flatnonzero(arr_bools)

        if sample_indices.size == 0:
            _log.debug(
                "Skipping ALT %s for variant line %d, no samples with ALT",
                alt, self._variant_row,
            )
            return False

        indices_list = sample_indices.tolist()
        parquet_row_data["INDIVIDUALS"] = self._range_str(indices_list)

        return True

    def _get_genotype(self, variant: Variant) -> np.ndarray:
        # array() returns numpy array, where last column indicates phased,
        # e.g. [0, 1, 1] is 0|1, hence here we remove last column.
        # Unknown alleles are represented by -1.
        # Extra cols of samples with lower ploidy are filled with -2.
        # variant.genotype is at least 2 times faster than variant.genotypes.
        genotype = variant.genotype.array(fill=-2)[:, :-1]

        # Human data: allow haploid samples; reject only ploidy > 2
        if genotype.shape[1] > 2:
            self._fail(f"Ploidy must not exceed 2, but is {genotype.shape[1]}")

        # Do not error on -2 values (padding for haploid samples)

        # Consider only non-negative allele indices when validating against ALT count
        positive = genotype[genotype >= 0]
        max_allele = int(positive.max()) if positive.size else -1
        len_alts = len(variant.ALT)

        if max_allele > len_alts:
            self._fail(
                f"Max GT allele index {max_allele} exceeds number of ALT alleles {len_alts}")

        return genotype

    @staticmethod
    def _range_str(indices: list[int]) -> str:
        assert len(indices) > 0, "Got an empty array of matching individuals."

        previous = indices[0]
        last = indices[-1]
        range_length = 0
        range_str = str(previous)

        for index in indices[1:]:
            if index - previous == 1:
                if index != last:
                    range_length += 1
                elif range_length > 0:
                    range_str += f"-{index}"
                else:
                    range_str += f",{index}"
            else:
                if range_length > 1:
                    range_str += f"-{previous},{index}"
                elif range_length == 1:
                    range_str += f",{previous},{index}"
                else:
                    range_str += f",{index}"
                range_length = 0
            previous = index

        return range_str
