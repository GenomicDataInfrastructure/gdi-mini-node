import logging
from logging import Formatter, StreamHandler, getLogger
from sys import stdout

from pythonjsonlogger.json import JsonFormatter

from .model import LoggerConfig, LoggerFormatEnum


def apply_logger_config(config: LoggerConfig) -> None:
    """Configures the application logger based on the provided configuration."""

    if config.format == LoggerFormatEnum.JSON:
        mandatory_fields = (
            "%(asctime)%(threadName)%(levelname)%(name)%(lineno)%(message)"
        )
        formatter = JsonFormatter(mandatory_fields)
    else:
        fmt = "%(asctime)s [%(threadName)s] %(levelname)-5s [%(name)s:%(lineno)d] %(message)s"
        formatter = Formatter(fmt)



    handler = StreamHandler(stdout)
    handler.setFormatter(formatter)
    handler.set_name("console")

    logging.root.setLevel(config.root_level.name)
    logging.root.handlers.clear()
    logging.root.addHandler(handler)
    logging.root.propagate = False

    logging.getLogger("uvicorn").propagate = False

    app_logger = getLogger(__name__.split(".")[0])
    app_logger.setLevel(config.app_level.name)
    app_logger.addHandler(handler)
    app_logger.propagate = False
