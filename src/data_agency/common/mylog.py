import logging

from logging.handlers import RotatingFileHandler
from pathlib import Path
import os
from .load_env import LOG_PATH

logs_dir = LOG_PATH


# logging.basicConfig(level=logging.INFO)
date_format = "%m-%d %H:%M:%S"
formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s", datefmt=date_format)
formatter = logging.Formatter("%(message)s", datefmt=date_format)
file_handler2 = RotatingFileHandler(logs_dir / "data_agency.log", maxBytes=10_000_000, backupCount=5)
file_handler2.setLevel(logging.INFO)
file_handler2.setFormatter(formatter)
logger = logging.getLogger("data_agency")
logger.addHandler(file_handler2)
logger.setLevel(logging.INFO)
logger.info("\n\n\nnew session started\n\n\n")


def get_logger() -> logging.Logger:
    return logger
