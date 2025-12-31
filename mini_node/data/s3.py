import hashlib
import logging
import threading
import time
from pathlib import Path
from typing import Set
from urllib.parse import urlparse

from iterators import TimeoutIterator
from minio import Minio
from minio.error import S3Error

from mini_node.data.fs import RegistryUpdater
from mini_node.data.registry import DataRegistry
from mini_node.setup.model import S3StorageConfig

_log = logging.getLogger(__name__)


class S3DataSync:
    """File-synchronizer for keeping the local data directory the same as in the
    specified S3 bucket.

    Also reports file-changes to the in-memory data-registry.
    """

    def __init__(
            self,
            s3_sync_config: S3StorageConfig,
            data_dir: str,
            registry: DataRegistry,
    ):
        """
        Initialises the class that synchronises local data-files based on the
        resources in the target S3 storage.

        The storage URL is specified as "[STORAGE-URL]/[BUCKET-NAME]/[PREFIX]"
        where the PREFIX is an optional path-prefix for the files to be synced.
        The PREFIX will be removed when resolving the local path of the files.

        Args:
            s3_sync_config: S3 access configuration
            data_dir: existing local directory to keep in sync
            registry: the in-memory file-registry to be updated on changes
        """
        self._data_dir = Path(data_dir).resolve()
        self._registry_updater = RegistryUpdater(self._data_dir, registry)
        if not self._data_dir.is_dir():
            raise ValueError(
                f"dest_dir does not exist or is not a directory: {data_dir}")

        parsed = urlparse(s3_sync_config.url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid S3 URL: {s3_sync_config.url}")

        endpoint = parsed.netloc
        secure = parsed.scheme == "https"

        path_parts = parsed.path.lstrip("/").split("/", 1)
        self._bucket = path_parts[0]
        self._suffix = s3_sync_config.path_suffix
        self._prefix = path_parts[1].lstrip("/") if len(path_parts) > 1 else ""

        if self._prefix and not self._prefix.endswith("/"):
            self._prefix += "/"

        self.client = Minio(
            region=s3_sync_config.region,
            endpoint=endpoint,
            access_key=s3_sync_config.access_key,
            secret_key=s3_sync_config.secret_key,
            secure=secure,
        )

    # ------------------------------------------------------------------ #
    # Utility methods
    # ------------------------------------------------------------------ #

    def _local_path_for_object(self, obj_name: str) -> Path:
        item_path = obj_name[len(self._prefix):] if self._prefix else obj_name
        return self._data_dir / item_path

    @staticmethod
    def _md5sum(path: Path) -> str:
        h = hashlib.md5()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()

    def _download_file(self, local_path: Path, obj_path: str):
        _log.info("Downloading %s -> %s", obj_path, local_path)
        self.client.fget_object(self._bucket, obj_path, str(local_path))
        self._registry_updater.on_new_file(local_path)

    def _remove_file(self, file_path: Path) -> None:
        """Removes a file and possibly its directory, if it becomes empty.

        Starts with the file directory and removes the empty directories all
        the way up to the configured data-directory.
        """
        if file_path.is_file():
            self._registry_updater.on_removed_file(file_path)
        file_path.unlink()

        # Cleanup empty dirs
        parent = file_path.parent
        while parent != self._data_dir and not any(parent.iterdir()):
            parent.rmdir()
            parent = parent.parent

    # ------------------------------------------------------------------ #
    # Sync logic
    # ------------------------------------------------------------------ #

    def sync(self) -> None:
        _log.info(
            "Starting full sync from bucket=%s prefix=%s to %s",
            self._bucket,
            self._prefix or "(root)",
            self._data_dir,
        )

        seen_local_paths: Set[Path] = set()

        objects = self.client.list_objects(
            self._bucket,
            prefix=self._prefix,
            recursive=True,
        )

        for obj in objects:
            local_path = self._local_path_for_object(obj.object_name)
            seen_local_paths.add(local_path)

            local_path.parent.mkdir(parents=True, exist_ok=True)
            needs_download = True

            if local_path.exists():
                if local_path.stat().st_size == obj.size:
                    if obj.etag and "-" not in obj.etag:
                        local_md5 = self._md5sum(local_path)
                        if local_md5 == obj.etag:
                            needs_download = False
                    else:
                        _log.warning(
                            "Multipart ETag [%s] for [%s], skipping MD5 check",
                            obj.etag, obj.object_name,
                        )
                        needs_download = False

            if needs_download:
                self._download_file(local_path, obj.object_name)

        # Remove stale local files
        for path in self._data_dir.rglob("*"):
            if path.is_file() and path not in seen_local_paths:
                _log.info("Deleting local file [%s] (not present in S3)", path)
                self._remove_file(path)

        # Remove empty directories
        for path in sorted(self._data_dir.rglob("*"), reverse=True):
            if path.is_dir() and not any(path.iterdir()):
                path.rmdir()

        _log.info("Full sync completed")

    # ------------------------------------------------------------------ #
    # Observe logic
    # ------------------------------------------------------------------ #

    def observe(self, stop_signal: threading.Event) -> None:
        """
        Runs an S3 events listener in current thread and keeps
        the local directory in sync until stop_signal is set.
        """

        _log.info(
            "Observing S3 storage for events in bucket=[%s]; path-prefix=[%s].",
            self._bucket,
            self._prefix or "(root)",
        )

        while not stop_signal.is_set():
            try:
                with self.client.listen_bucket_notification(
                        self._bucket,
                        prefix=self._prefix,
                        suffix=self._suffix,
                        events=("s3:ObjectCreated:*", "s3:ObjectRemoved:*"),
                ) as events:
                    it = TimeoutIterator(events, timeout=1.5)

                    for event in it:
                        if stop_signal.is_set():
                            break

                        # Sentinel is returned when timeout occurred:
                        if event is it.get_sentinel():
                            continue

                        for record in event.get("Records", []):
                            event_name = record["eventName"]
                            obj = record["s3"]["object"]["key"]
                            local_path = self._local_path_for_object(obj)

                            if event_name.startswith("s3:ObjectCreated"):
                                _log.info("S3 created: %s", obj)
                                local_path.parent.mkdir(parents=True,
                                                        exist_ok=True)
                                self._download_file(local_path, obj)

                            elif event_name.startswith("s3:ObjectRemoved"):
                                _log.info("S3 removed: %s", obj)
                                if local_path.exists():
                                    self._remove_file(local_path)

            except Exception as exc:
                _log.error("S3 event-listener error: %s", exc)
                time.sleep(60)

        _log.info("S3 event observer stopped")
