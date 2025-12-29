from threading import Event

from .config import fdp_config
from .registry import DataRegistry, scan_data_directory
from ..setup import s3_sync_config, app_data_dir

DATA = DataRegistry(fdp_config.catalogs)


def monitor_files(stop_signal: Event) -> None:
    scan_data_directory(DATA)

    # Depending on whether an S3 storage is configured or not,
    # either an S3 storage observer or a local file-system observer is started.

    if s3_sync_config is not None and s3_sync_config.is_enabled():
        from .s3 import S3DataSync
        monitor = S3DataSync(s3_sync_config, app_data_dir, DATA)
        monitor.sync()  # Synchronises the data directory from an S3 bucket
    else:
        from .fs import DataDirectoryObserver
        monitor = DataDirectoryObserver(app_data_dir, DATA)

    # Reports the current status of the data-registry.
    DATA.log_status()

    # Starts an obsever process, which blocks the current thread.
    monitor.observe(stop_signal)


__all__ = ["DATA", "monitor_files"]
