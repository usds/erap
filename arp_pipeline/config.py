import os
from pathlib import Path

from dotenv import dotenv_values

CONFIG = {
    **dotenv_values(Path(os.path.dirname(__file__)) / ".." / ".env"),
    **os.environ,
}


def get_db_connection_string() -> str:
    assert "DB_CONN" in CONFIG and CONFIG["DB_CONN"] is not None
    return CONFIG["DB_CONN"]
