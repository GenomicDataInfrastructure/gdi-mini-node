from threading import Event

from .config import fdp_config
from .data import DataRegistry, scan_data_directory
from ..setup import s3_sync_config, app_data_dir

DATA = DataRegistry(fdp_config.catalogs)


def monitor_files(stop_signal: Event) -> None:
    scan_data_directory(DATA)

    if s3_sync_config is not None and s3_sync_config.is_enabled():
        from .s3 import S3DataSync
        s3_data = S3DataSync(s3_sync_config, app_data_dir)
        s3_data.sync()
        s3_data.observe(stop_signal)


__all__ = ["DATA", "monitor_files"]
