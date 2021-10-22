import os
from enum import Enum
from pathlib import Path

from dotenv import dotenv_values

CONFIG = {
    **dotenv_values(Path(os.path.dirname(__file__)) / ".." / ".env"),
    **os.environ,
}


def get_db_connection_string() -> str:
    assert "DB_CONN" in CONFIG and CONFIG["DB_CONN"] is not None
    return CONFIG["DB_CONN"]


class ACSVariable(str, Enum):
    MEDIAN_RENTER_INCOME = "B25119_003E"


def get_storage_path() -> str:
    if "STORAGE_PATH" in CONFIG:
        storage_path = CONFIG["STORAGE_PATH"]
    else:
        storage_path = os.getcwd()
    return os.path.abspath(storage_path)
