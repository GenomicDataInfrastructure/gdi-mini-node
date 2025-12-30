import os
from dataclasses import dataclass, field
from enum import StrEnum
from logging import getLogger
from os.path import abspath, basename, dirname, isdir, isfile, join

import yaml

from mini_node.fdp.config import FdpCatalog, FdpDataset
from mini_node.setup import app_data_dir

_log = getLogger(__name__)

POS_DIVIDER = 10_000_000


@dataclass
class FdpData:
    """FAIR Data Point instance data"""
    catalogs: dict[str, FdpCatalog] = field(default_factory=lambda: {})
    datasets: dict[str, FdpDataset] = field(default_factory=lambda: {})
    catalog_datasets: dict[str, list[str]] = field(default_factory=lambda: {})


class BeaconAssembly(StrEnum):
    """Assembly values that we support (case-sensitive)."""
    GRCh37 = "GRCh37"
    GRCh38 = "GRCh38"


@dataclass
class BeaconDataset:
    """A dataset in the Beacon."""
    dataset_id: str
    individuals_parquet: str | None = None
    chr_group_files: dict[str, str] = field(default_factory=lambda: {})


@dataclass
class BeaconData:
    """Beacon data managements starts with assemblies.
    Each Beacon instance has their own instance of BeaconData.
    """
    assemblies: dict[BeaconAssembly, list[BeaconDataset]] = field(
        default_factory=dict)

    def get_dataset_ids(self) -> list[str]:
        dataset_ids = set()
        for assembly in self.assemblies:
            for dataset in self.assemblies[assembly]:
                dataset_ids.add(dataset.dataset_id)
        return list(sorted(dataset_ids))

    def get_dataset_individuals(
            self,
            assembly: BeaconAssembly | None = None,
            chrom: str | None = None,
            pos: int | None = None,
    ) -> dict[str, tuple[str, str]]:
        datasets = {}
        if assembly is not None and assembly not in self.assemblies:
            return datasets

        chr_group = None
        if assembly is not None and chrom is not None and pos is not None:
            group = pos // POS_DIVIDER
            chr_group = f"{chrom}.{group}"

        for assembly in self.assemblies:
            for dataset in self.assemblies[assembly]:
                # "individuals.parquet" is always mandatory:
                parquet1 = dataset.individuals_parquet
                if parquet1 is None:
                    _log.warning(
                        "Dataset %s is missing its individuals.parquet file",
                        dataset.dataset_id,
                    )
                    continue

                parquet2 = None
                if chr_group is not None:
                    parquet2 = dataset.chr_group_files.get(chr_group)

                    # "individuals-chrX.Y.parquet" is mandatory when variant
                    # position was provided to this method.
                    if parquet2 is None:
                        continue

                datasets[dataset.dataset_id] = (parquet1, parquet2)

        _log.debug(
            "get_dataset_individuals('%s', (%s, %s) -> %s) -> %d datasets",
            assembly, chrom, pos, chr_group, len(datasets))

        return datasets

    def get_dataset_files(
            self, assembly: BeaconAssembly, chrom: str, pos: int,
    ) -> dict[str, str]:
        datasets = {}
        if assembly not in self.assemblies:
            return datasets

        group = pos // POS_DIVIDER
        chr_group = f"{chrom}.{group}"

        for dataset in self.assemblies[assembly]:
            file_path = dataset.chr_group_files.get(chr_group)
            if file_path is not None:
                datasets[dataset.dataset_id] = file_path

        _log.debug("get_dataset_files('%s', (%s, %s) -> %s) -> %d datasets",
                   assembly, chrom, pos, chr_group, len(datasets))

        return datasets


# This class relies on certain expectation about file-paths:
# <some-path>/DATASET_ID/
#   metadata.yaml – FDP properties about the datasets
#   ASSEMBLY/     – either GRCh37 or GRCh38
#     {allele-freq|individuals}-chr{C}.{I}.parquet – Beacon data files
#     individuals.parquet – Beacon data files about dataset individuals
# Not matching files are ignored.
class DataRegistry:
    def __init__(self, catalogs: dict[str, FdpCatalog]) -> None:
        self.fdp = FdpData()
        self.aggregated_beacon = BeaconData()
        self.sensitive_beacon = BeaconData()
        self.problematic_files: dict[str, str] = {}

        for catalog_id, catalog in catalogs.items():
            self.fdp.catalogs[catalog_id] = catalog

    def forget_issues_with(self, file_path: str) -> None:
        if file_path in self.problematic_files:
            del self.problematic_files[file_path]

    def forget_issues_in_dir(self, dir_path: str) -> None:
        paths = set(self.problematic_files.keys())
        for file_path in paths:
            if file_path.startswith(dir_path):
                del self.problematic_files[file_path]

    def record_issues_with(self, file_path: str, e: Exception) -> None:
        self.problematic_files[file_path] = repr(e)
        _log.warning("Problematic file [%s]: %s", file_path, e)

    def add_dataset(self, dataset_id: str, props: FdpDataset) -> None:
        self.fdp.datasets[dataset_id] = props

        catalog_id = props.catalog_id
        if catalog_id is None:
            _log.warning("[add_dataset] %s is missing catalog_id", dataset_id)
            return

        # Make sure it's not already included in another catalogue:
        for dataset_ids in self.fdp.catalog_datasets.values():
            if dataset_id in dataset_ids:
                dataset_ids.remove(dataset_id)

        # Register the dataset to the catalogue:
        if catalog_id not in self.fdp.catalog_datasets:
            self.fdp.catalog_datasets[catalog_id] = []
        self.fdp.catalog_datasets[catalog_id].append(dataset_id)
        _log.debug("[add_dataset] %s to catalog %s", dataset_id, catalog_id)

        if catalog_id not in self.fdp.catalogs:
            _log.warning("[add_dataset] %s references catalog_id [%s], which "
                         "is not defined in FDP configuration, thus the "
                         "dataset is not visible.", dataset_id, catalog_id)

    def remove_dataset(self, dataset_id: str, also_beacon_data=False) -> None:
        found_in_catalog = False
        for catalog_id, dataset_ids in self.fdp.catalog_datasets.items():
            if dataset_id in dataset_ids:
                dataset_ids.remove(dataset_id)
                _log.debug("[remove_dataset] %s from catalog %s",
                           dataset_id, catalog_id)
                found_in_catalog = True

        if dataset_id in self.fdp.datasets:
            del self.fdp.datasets[dataset_id]

        if not found_in_catalog:
            _log.debug("[remove_dataset] %s from FDP", dataset_id)

        # By default, the Beacon data is not modified as the metadata file might
        # be added later (temporary delete and updated). In that case, the
        # Beacon data does not have to scanned again.
        if not also_beacon_data:
            return

        for beacon_data in [self.aggregated_beacon, self.sensitive_beacon]:
            for assembly in beacon_data.assemblies:
                datasets = beacon_data.assemblies[assembly]
                for dataset in list(datasets):
                    if dataset.dataset_id == dataset_id:
                        datasets.remove(dataset)
                        _log.debug("[remove_dataset] %s from assembly %s",
                                   dataset_id, assembly)

    def remove_beacon_dataset(self, dataset_id: str, assembly: BeaconAssembly):
        for beacon_data in [self.aggregated_beacon, self.sensitive_beacon]:
            if assembly not in beacon_data.assemblies:
                continue

            datasets = beacon_data.assemblies[assembly]
            for dataset in list(datasets):
                if dataset.dataset_id == dataset_id:
                    datasets.remove(dataset)
                    _log.debug("[remove_beacon_dataset] %s from assembly %s",
                               dataset_id, assembly)

    def add_parquet(
            self,
            dataset_id: str,
            assembly: BeaconAssembly,
            file_path: str,
    ) -> None:
        filename = basename(file_path)
        target_dataset = self._resolve_beacon_dataset(filename, assembly,
                                                      dataset_id)
        if target_dataset is None:
            _log.warning("[add_parquet] Ignoring Parquet file due unsupported "
                         "prefix [%s]", file_path)
            return

        if filename == "individuals.parquet":
            target_dataset.individuals_parquet = file_path
            _log.debug("[add_parquet] %s file %s", dataset_id, file_path)
            return

        chr_group = self._resolve_chr_group(filename)
        if chr_group is None:
            _log.warning("[add_parquet] Ignoring Parquet file due bad "
                         "chr-group [%s]", file_path)
            return

        target_dataset.chr_group_files[chr_group] = file_path
        _log.debug("[add_parquet] %s file %s to assembly %s",
                   dataset_id, file_path, assembly)

    def remove_parquet(self, dataset_id: str, file_path: str) -> None:
        filename = basename(file_path)
        assembly = BeaconAssembly(basename(dirname(file_path)))
        target_dataset = self._resolve_beacon_dataset(
            filename, assembly, dataset_id)
        if target_dataset is None:
            return

        if filename == "individuals.parquet":
            _log.debug("[remove_parquet] %s file %s", dataset_id, file_path)
            target_dataset.individuals_parquet = None
        else:
            chr_group = self._resolve_chr_group(filename)
            if chr_group is None:
                _log.warning(
                    "[remove_parquet] Ignoring Parquet file due bad chr-group [%s]",
                    file_path)
            else:
                if chr_group in target_dataset.chr_group_files:
                    del target_dataset.chr_group_files[chr_group]
                _log.debug("[remove_parquet] %s file %s from assembly %s",
                           dataset_id, file_path, assembly)

        if target_dataset.individuals_parquet is None and \
                len(target_dataset.chr_group_files) == 0:
            self.remove_beacon_dataset(dataset_id, assembly)

    def _resolve_beacon_dataset(
            self,
            filename: str,
            assembly: BeaconAssembly | None,
            dataset_id: str,
    ) -> BeaconDataset | None:
        if filename.startswith("allele-freq-"):
            beacon_data = self.aggregated_beacon
        elif filename.startswith("individuals-"):
            beacon_data = self.sensitive_beacon
        elif filename == "individuals.parquet":
            beacon_data = self.sensitive_beacon
        else:
            _log.warning("Ignoring Parquet file due to non-standard name [%s]",
                         filename)
            return None

        # When assembly is defined, the dataset needs to be added, if missing.
        if assembly is not None:
            if assembly not in beacon_data.assemblies:
                beacon_data.assemblies[assembly] = []

            datasets = beacon_data.assemblies[assembly]
            for dataset in datasets:
                if dataset.dataset_id == dataset_id:
                    return dataset

            dataset = BeaconDataset(dataset_id)
            datasets.append(dataset)
            return dataset

        for assembly in beacon_data.assemblies:
            for dataset in beacon_data.assemblies[assembly]:
                if dataset.dataset_id == dataset_id:
                    return dataset
        return None

    @staticmethod
    def _resolve_chr_group(filename: str) -> str | None:
        start = filename.rindex("chr") + 3
        end = filename.rindex(".")
        return filename[start: end] if 0 < start < end else None

    def log_status(self):
        _log.info("Data scanning completed: %d datasets in total",
                  len(self.fdp.datasets))
        _log.info("Aggregated Beacon: %d datasets",
                  len(self.aggregated_beacon.get_dataset_ids()))
        _log.info("Sensitive Beacon: %d datasets",
                  len(self.sensitive_beacon.get_dataset_ids()))


def scan_data_directory(registry: DataRegistry) -> None:
    for dataset_id in os.listdir(app_data_dir):
        metadata_file = join(app_data_dir, dataset_id, "metadata.yaml")
        if isfile(metadata_file):
            try:
                registry.forget_issues_with(metadata_file)
                yaml_data = yaml.safe_load(open(metadata_file, "r"))
                dataset_meta = FdpDataset(**yaml_data)
                registry.add_dataset(dataset_id, dataset_meta)
            except Exception as e:
                registry.record_issues_with(metadata_file, e)
                _log.exception("Error parsing metadata from %s", metadata_file)

        for assembly in BeaconAssembly:
            assembly_dir = join(app_data_dir, dataset_id, assembly)
            if isdir(assembly_dir):
                for parquet_file in os.listdir(assembly_dir):
                    if parquet_file.endswith(".parquet"):
                        abs_path = abspath(join(assembly_dir, parquet_file))
                        registry.add_parquet(dataset_id, assembly, abs_path)
