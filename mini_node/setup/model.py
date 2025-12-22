from enum import Enum

from pydantic import BaseModel

"""Data model of the app.yaml configuration."""


class LoggerLevelEnum(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"


class LoggerFormatEnum(str, Enum):
    PLAIN = "plain"
    JSON = "json"


class LoggerConfig(BaseModel):
    root_level: LoggerLevelEnum
    app_level: LoggerLevelEnum
    format: LoggerFormatEnum


class S3StorageConfig(BaseModel):
    url: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    region: str = "us-east-1"
    path_suffix: str = ".parquet"

    def is_enabled(self) -> bool:
        return self.url is not None \
            and self.access_key is not None \
            and self.secret_key is not None

class BasicAuthConfig(BaseModel):
    username: str | None = None
    password: str | None = None


class AppConfig(BaseModel):
    logger: LoggerConfig
    data_dir: str
    sync_from_s3: S3StorageConfig | None = None
    basic_auth: BasicAuthConfig | None = None
