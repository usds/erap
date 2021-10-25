import os
from enum import Enum
from pathlib import Path
from typing import Optional

from dotenv import dotenv_values

CONFIG = {
    **dotenv_values(Path(os.path.dirname(__file__)) / ".." / ".env"),
    **os.environ,
}


class ACSVariable(str, Enum):
    MEDIAN_RENTER_INCOME = "B25119_003E"


def get_storage_path(storage_subdir: Optional[str] = None) -> str:
    if "STORAGE_PATH" in CONFIG:
        storage_path = CONFIG["STORAGE_PATH"]
    else:
        storage_path = os.getcwd()
    base_storage_path = os.path.abspath(storage_path)
    if storage_subdir is not None:
        return os.path.join(base_storage_path, storage_subdir)
    else:
        return base_storage_path


DEFAULT_CENSUS_YEAR = int(CONFIG.get("DEFAULT_CENSUS_YEAR", 2019))
