import threading
import time
from logging import getLogger
from pathlib import Path

import yaml
from watchdog.events import DirDeletedEvent, DirModifiedEvent, DirMovedEvent, \
    FileDeletedEvent, FileModifiedEvent, FileMovedEvent, LoggingEventHandler
from watchdog.observers import Observer

from mini_node.data.registry import BeaconAssembly, DataRegistry
from mini_node.fdp.config import FdpDataset

_log = getLogger(__name__)


class DataDirectoryObserver:
    """Observer for file-system changes in given data directory.

    Reports the changes to the in-memory data-registry.
    """

    def __init__(self, data_dir: str, registry: DataRegistry) -> None:
        self._data_dir = Path(data_dir).resolve()
        self._updater = RegistryUpdater(self._data_dir, registry)

    def observe(self, stop_signal: threading.Event) -> None:
        event_handler = FileRegistryUpdater(self._updater)
        observer = Observer()
        observer.schedule(event_handler, self._data_dir, recursive=True)
        observer.start()
        _log.info("Observing directory for changes: %s", self._data_dir)
        try:
            while not stop_signal.is_set():
                time.sleep(1)
        finally:
            observer.stop()
            observer.join()


class FileRegistryUpdater(LoggingEventHandler):
    """Provides methods that handle file-system events. The parent class ensures
    that the events are logged to the provided logger. Only a subset of all
    possible events are covered here. Others where not covered as they seemed
    to be irrelevant.
    """

    def __init__(self, updater: RegistryUpdater) -> None:
        super().__init__(logger=_log)
        self._updater = updater

    def on_modified(self, event: DirModifiedEvent | FileModifiedEvent) -> None:
        if not event.is_directory:
            super().on_modified(event)
            self._updater.on_new_file(Path(event.src_path))

    def on_deleted(self, event: DirDeletedEvent | FileDeletedEvent) -> None:
        super().on_deleted(event)
        if not event.is_directory:
            self._updater.on_removed_file(Path(event.src_path))

    def on_moved(self, event: DirMovedEvent | FileMovedEvent) -> None:
        super().on_moved(event)
        if event.is_directory:
            self._updater.on_moved_dir(
                Path(event.src_path),
                Path(event.dest_path),
            )
        else:
            self._updater.on_removed_file(Path(event.src_path))
            self._updater.on_new_file(Path(event.dest_path))


class RegistryUpdater:
    """Helper-class for registering file-system changes in the DataRegistry.
    Note that this class is following the data-directory structure to avoid
    processing file-paths that do not conform.
    """

    def __init__(self, data_dir: Path, registry: DataRegistry) -> None:
        self._data_dir = data_dir.resolve()
        self._registry = registry

    def on_new_file(self, file_path: Path) -> None:
        if file_path.name == "metadata.yaml":
            self._on_new_metadata(file_path)
        elif file_path.suffix == ".parquet":
            self._on_new_parquet(file_path)

    def on_removed_file(self, file_path: Path) -> None:
        if file_path.name == "metadata.yaml":
            self._on_removed_metadata(file_path)
        elif file_path.suffix == ".parquet":
            self._on_removed_parquet(file_path)

    def _on_new_metadata(self, file_path: Path):
        dataset_id = self._resolve_dataset(file_path)
        if dataset_id is None:
            return

        try:
            with open(file_path, "r") as stream:
                yaml_data = yaml.safe_load(stream)
                dataset_meta = FdpDataset(**yaml_data)
                self._registry.add_dataset(dataset_id, dataset_meta)
        except Exception as e:
            full_path = file_path.resolve().as_posix()
            self._registry.record_issues_with(full_path, e)

    def _on_removed_metadata(self, file_path: Path):
        dataset_id = self._resolve_dataset(file_path)
        if dataset_id is None:
            return

        full_path = file_path.resolve().as_posix()
        self._registry.forget_issues_with(full_path)
        self._registry.remove_dataset(dataset_id, also_beacon_data=True)

    def _on_new_parquet(self, file_path: Path):
        dataset_id, assembly = self._resolve_dataset_assembly(file_path)
        if dataset_id is None:
            return

        full_path = file_path.resolve().as_posix()
        self._registry.add_parquet(dataset_id, assembly, full_path)

    def _on_removed_parquet(self, file_path: Path):
        dataset_id, assembly = self._resolve_dataset_assembly(file_path)
        if dataset_id is None:
            return

        full_path = file_path.resolve().as_posix()
        self._registry.forget_issues_with(full_path)
        self._registry.remove_parquet(dataset_id, full_path)

    def on_moved_dir(self, src_path: Path, dest_path: Path):
        # Check if a dataset-directory was renamed
        if src_path.parent == self._data_dir:
            self._registry.remove_dataset(src_path.name, also_beacon_data=True)

        # Check if an assembly-directory was renamed
        elif src_path.name in BeaconAssembly:
            assembly = BeaconAssembly(src_path.name)
            dataset_id = src_path.parent.name
            self._registry.remove_dataset(dataset_id, assembly)

        self._registry.forget_issues_in_dir(src_path.resolve().as_posix())

        # Check if a new dataset-directory was created
        if dest_path.parent == self._data_dir:
            metadata = dest_path / "metadata.yaml"
            if metadata.exists():
                self._on_new_metadata(metadata)

            dataset_id = dest_path.name
            for assembly in BeaconAssembly:
                assembly_dir = dest_path / assembly
                if assembly_dir.is_dir():
                    self._include_assembly_dir(dataset_id, assembly_dir)

        # Check if a new assembly-directory was created
        elif dest_path.name in BeaconAssembly:
            dataset_id = dest_path.parent.name
            self._include_assembly_dir(dataset_id, dest_path)

    def _resolve_dataset(self, file_path: Path):
        dataset_dir = file_path.parent

        if dataset_dir.parent != self._data_dir:
            _log.warning("Ignoring metadata file as its parent-directory is "
                         "not a sub-directory of the data-directory: %s",
                         file_path)
            return None

        return dataset_dir.name

    def _resolve_dataset_assembly(self, file_path: Path):
        assembly_dir = file_path.parent
        if assembly_dir.name not in BeaconAssembly:
            _log.warning("Ignoring Parquet file as its parent-directory does "
                         "not specify a valid assembly (GRCh37, GRCh38): %s",
                         file_path)
            return None, None

        dataset_dir = assembly_dir.parent
        if dataset_dir.parent != self._data_dir:
            _log.warning("Ignoring Parquet file as its dataset-directory is "
                         "not a sub-directory of the data-directory: %s",
                         file_path)
            return None, None

        return dataset_dir.name, BeaconAssembly(assembly_dir.name)

    def _include_assembly_dir(self, dataset_id: str, assembly_dir: Path):
        assembly = BeaconAssembly(assembly_dir.name)
        for parquet_file in assembly_dir.glob("*.parquet"):
            abs_path = parquet_file.resolve().as_posix()
            self._registry.add_parquet(dataset_id, assembly, abs_path)


__ALL__ = ["DataDirectoryObserver", "RegistryUpdater"]
