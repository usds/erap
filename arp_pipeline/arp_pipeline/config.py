from enum import Enum
import os
from pathlib import Path
from typing import Optional

from dotenv import dotenv_values

CONFIG = {
    **dotenv_values(Path(os.path.dirname(__file__)) / ".." / ".env"),
    **os.environ,
}


def get_db_connection_string(db_year: Optional[str] = None) -> str:
    assert "DB_CONN" in CONFIG and CONFIG["DB_CONN"] is not None
    db_conn = CONFIG["DB_CONN"]
    if db_year is not None:
        db_conn = "_".join([db_conn, db_year])
    return db_conn


class ACSVariable(str, Enum):
    MEDIAN_RENTER_INCOME = "B25119_003E"
