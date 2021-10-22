import os
from enum import Enum
from typing import Iterable, Union
from zipfile import ZipFile

import luigi
from luigi.contrib.postgres import PostgresTarget
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import text

from arp_pipeline.config import get_db_connection_string, get_storage_path
from arp_pipeline.download_utils import download_zip
from arp_pipeline.tiger_utils import get_shp2pgsql_cmd, run_raw_sql, staging_schema

DB_CONN = get_db_connection_string()


class DataScope(str, Enum):
    LOCAL = "local"
    NATIONAL = "national"


class DownloadTigerData(luigi.Task):
    year: int = luigi.IntParameter(default=2019)
    feature_name: str = luigi.Parameter()
    data_scope: DataScope = luigi.EnumParameter(
        enum=DataScope, default=DataScope.NATIONAL
    )
    retry_count = 5

    @property
    def file_name(self) -> str:
        return f"tl_{self.year}_us_{self.feature_name.lower()}.zip"

    @property
    def source_path(self) -> str:
        """The path on www2.census.gov/geo/tiger/TIGER20NN/ to find the files"""
        return f"{self.feature_name.upper()}/{self.file_name}"

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            os.path.join(
                get_storage_path(),
                f"tiger/{self.year}/{self.data_scope}/{self.feature_name}/{self.file_name}",
            ),
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        url = f"https://www2.census.gov/geo/tiger/TIGER{self.year}/{self.source_path}"
        with self.output().open("wb") as f:
            f.write(download_zip(url))


class UnzipNationalTigerData(luigi.Task):
    year = luigi.IntParameter(default=2019)
    feature_name = luigi.Parameter()
    data_scope: DataScope = luigi.EnumParameter(
        enum=DataScope, default=DataScope.NATIONAL
    )

    def requires(self):
        return DownloadTigerData(year=self.year, feature_name=self.feature_name)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            os.path.join(
                get_storage_path(),
                (
                    f"tiger/{self.year}/{self.data_scope}/{self.feature_name.lower()}/"
                    f"tl_{self.year}_us_{self.feature_name.lower()}.dbf"
                ),
            ),
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        zip_path = os.path.abspath(self.input().path)
        with ZipFile(zip_path) as feature_zip:
            os.chdir(os.path.dirname(zip_path))
            feature_zip.extractall()


class LoadStateData(luigi.Task):
    year = luigi.IntParameter(default=2019)
    resources = {"max_workers": 1}

    def requires(self) -> UnzipNationalTigerData:
        return UnzipNationalTigerData(year=self.year, feature_name="state")

    def output(self) -> PostgresTarget:
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table="tiger_data.state_all",
            update_id="tiger_state_dataload",
        )

    def run(self) -> None:

        dbf_file_path = os.path.abspath(self.input().path)
        dbf_file_dir = os.path.dirname(dbf_file_path)

        cmd_chain = get_shp2pgsql_cmd(DB_CONN, dbf_file_path, "tiger_staging.state_all")
        with self.output().engine.connect() as connection:
            run_sql = lambda statement: connection.execute(text(statement))
            with connection.begin():
                run_sql(f"UPDATE tiger.loader_variables SET tiger_year='{self.year}';")
            with staging_schema(connection, "state"):
                with connection.begin():
                    run_sql("DROP TABLE IF EXISTS tiger_data.state_all;")
                    run_sql(
                        """CREATE TABLE tiger_data.state_all(
                            CONSTRAINT pk_state_all PRIMARY KEY (statefp),
                            CONSTRAINT uidx_state_all_stusps UNIQUE (stusps),
                            CONSTRAINT uidx_state_all_gid UNIQUE (gid)
                            )
                        INHERITS(tiger.state);
                    """
                    )
                cmd_chain.with_cwd(dbf_file_dir)()
                with connection.begin():
                    run_sql(
                        "SELECT loader_load_staged_data(lower('state_all'), lower('state_all'));"
                    )
                    run_sql(
                        "CREATE INDEX tiger_data_state_all_the_geom_gist ON tiger_data.state_all USING gist(the_geom);"
                    )
            run_raw_sql(self.output().engine, "VACUUM ANALYZE tiger_data.state_all")
            with connection.begin():
                self.output().touch()


class LoadCountyData(luigi.Task):
    year = luigi.IntParameter(default=2019)
    resources = {"max_workers": 1}

    def requires(self) -> UnzipNationalTigerData:
        return UnzipNationalTigerData(year=self.year, feature_name="county")

    def output(self) -> PostgresTarget:
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table="tiger_data.county_all",
            update_id="tiger_county_dataload",
        )

    def run(self) -> None:
        dbf_file_path = os.path.abspath(self.input().path)
        dbf_file_dir = os.path.dirname(dbf_file_path)
        cmd_chain = get_shp2pgsql_cmd(
            DB_CONN, dbf_file_path, "tiger_staging.county_all"
        )

        with self.output().engine.connect() as connection:
            run_sql = lambda statement: connection.execute(text(statement))
            with staging_schema(connection, "county"):
                with connection.begin():
                    run_sql("DROP TABLE IF EXISTS tiger_data.county_all;")
                    run_sql(
                        """
                        CREATE TABLE tiger_data.county_all(
                            CONSTRAINT pk_tiger_data_county_all PRIMARY KEY (cntyidfp),
                            CONSTRAINT uidx_tiger_data_county_all_gid UNIQUE (gid)
                        ) INHERITS(tiger.county);
                        """
                    )
                cmd_chain.with_cwd(dbf_file_dir)()
                with connection.begin():
                    run_sql(
                        "ALTER TABLE tiger_staging.county_all RENAME geoid TO cntyidfp;"
                    )
                    run_sql(
                        "SELECT loader_load_staged_data(lower('county_all'), lower('county_all'));"
                    )
                    run_sql(
                        "CREATE INDEX tiger_data_county_the_geom_gist ON tiger_data.county_all USING gist(the_geom);"
                    )
                    run_sql(
                        """CREATE UNIQUE INDEX uidx_tiger_data_county_all_statefp_countyfp
                        ON tiger_data.county_all USING btree(statefp, countyfp);"""
                    )
                run_raw_sql(
                    self.output().engine, "VACUUM ANALYZE tiger_data.county_all;"
                )
                with connection.begin():
                    run_sql("DROP TABLE IF EXISTS tiger_data.county_all_lookup")
                    run_sql(
                        "CREATE TABLE tiger_data.county_all_lookup (CONSTRAINT pk_county_all_lookup PRIMARY KEY (st_code, co_code)) INHERITS (tiger.county_lookup);"
                    )
                    run_sql(
                        """INSERT INTO tiger_data.county_all_lookup(st_code, state, co_code, name)
                            SELECT CAST(s.statefp as integer), s.abbrev, CAST(c.countyfp as integer), c.name
                                FROM tiger_data.county_all As c
                                INNER JOIN state_lookup As s ON s.statefp = c.statefp;
                        """
                    )
            run_raw_sql(
                self.output().engine, "VACUUM ANALYZE tiger_data.county_all_lookup;"
            )
            with connection.begin():
                self.output().touch()


class LoadNationalData(luigi.WrapperTask):
    year = luigi.IntParameter(default=2019)

    def requires(self) -> Iterable[Union[LoadCountyData, LoadStateData]]:
        yield LoadStateData(year=self.year)
        yield LoadCountyData(year=self.year)
