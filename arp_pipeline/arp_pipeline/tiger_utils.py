from contextlib import contextmanager
from enum import Enum
from typing import Generator, Optional

from plumbum.cmd import psql, shp2pgsql
from plumbum.commands.base import Pipeline
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine


@contextmanager
def staging_schema(
    connection: Connection, namespace: Optional[str] = None
) -> Generator:
    run_sql = lambda statement: connection.execute(text(statement))
    if namespace is None:
        namespace = ""
    else:
        namespace = f"_{namespace}"

    with connection.begin():
        run_sql("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")
        run_sql("CREATE SCHEMA tiger_staging;")
    try:
        yield
    finally:
        with connection.begin():
            run_sql("DROP SCHEMA IF EXISTS tiger_staging CASCADE;")


def run_raw_sql(engine: Engine, statement: str) -> None:
    connection = engine.raw_connection()
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    cursor.execute(statement)


def create_indexes_and_vacuum(engine: Engine) -> None:
    run_raw_sql(engine, "SELECT install_missing_indexes();")
    run_raw_sql(engine, "vacuum (analyze) tiger.addr;")
    run_raw_sql(engine, "vacuum (analyze) tiger.edges;")
    run_raw_sql(engine, "vacuum (analyze) tiger.faces;")
    run_raw_sql(engine, "vacuum (analyze) tiger.featnames;")
    run_raw_sql(engine, "vacuum (analyze) tiger.place;")
    run_raw_sql(engine, "vacuum (analyze) tiger.cousub;")
    run_raw_sql(engine, "vacuum (analyze) tiger.county;")
    run_raw_sql(engine, "vacuum (analyze) tiger.state;")
    run_raw_sql(engine, "vacuum (analyze) tiger.zip_lookup_base;")
    run_raw_sql(engine, "vacuum (analyze) tiger.zip_state;")
    run_raw_sql(engine, "vacuum (analyze) tiger.zip_state_loc;")


class Shp2PGSqlMode(str, Enum):
    CREATE = "-c"
    APPEND = "-a"


def get_shp2pgsql_cmd(
    db_conn: str,
    dbf_file_name: str,
    table_name: str,
    mode: Shp2PGSqlMode = Shp2PGSqlMode.CREATE,
    srid: str = "4269",
) -> Pipeline:
    psql_cmd = psql[db_conn]
    shp2pgsql_cmd = shp2pgsql[
        "-D",
        mode.value,
        "-s",
        srid,
        "-g",
        "the_geom",
        "-W",
        "latin1",
        dbf_file_name,
        table_name,
    ]
    return shp2pgsql_cmd | psql_cmd
