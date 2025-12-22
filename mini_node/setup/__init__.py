import base64
from logging import getLogger
from typing import Type
from os.path import abspath, join
from sys import exit
from tomllib import load

from yaml import safe_load, YAMLError

from .logger import apply_logger_config
from .minio import MinioClient
from .model import S3StorageConfig, LoggerConfig, AppConfig

"""Provides common config-loading mechanism and loads the core configuration
from app.yaml."""


def load_config_yaml(filename, t: Type):
    filepath = join("config", filename)
    full_path = abspath(filepath)
    try:
        with open(filepath) as stream:
            config = safe_load(stream)
        assert isinstance(config, dict), "Content must be a dict: %s" % filepath
        return t(**config)
    except FileNotFoundError:
        if t == AppConfig:
            print("[ERROR] The configuration file is missing:", full_path)
            exit(1)
        print("[WARN] The configuration file is missing (module disabled):", full_path)
        return None
    except YAMLError as exc:
        print("[ERROR] Failed to parse configuration (YAML):", exc)
    except Exception as exc:
        print("[ERROR] Failed to process", full_path, "file:", exc)
    exit(1)


def encode_basic_credential(username: str, password: str) -> str:
    cred = (username + ":" + password).encode("utf-8")
    return "Basic " + base64.b64encode(cred).decode("ascii")


with open("pyproject.toml", "rb") as pyproject:
    app_version = load(pyproject)["project"]["version"]

app_config = load_config_yaml("app.yaml", AppConfig)
apply_logger_config(app_config.logger)

log = getLogger(__name__)
log.info("Logging is now configured. Welcome!")
log.debug("DEBUG-level logging is enabled.")

# node_minio_client = MinioClient(**config.services.minio.model_dump())

app_data_dir = app_config.data_dir
s3_sync_config = app_config.sync_from_s3

info_page_credentials = None
basic_conf = app_config.basic_auth
if basic_conf and basic_conf.username and basic_conf.password:
    info_page_credentials = encode_basic_credential(
        basic_conf.username, basic_conf.password)
    log.info("The root path is protected by BASIC authentication.")

__all__ = [
    "app_data_dir",
    "app_version",
    "encode_basic_credential",
    "info_page_credentials",
    "s3_sync_config",
]
