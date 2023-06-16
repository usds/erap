import os
from zipfile import ZipFile

import luigi
from luigi.contrib.sqla import SQLAlchemyTarget
from plumbum.cmd import ogr2ogr
from sqlalchemy import text

from arp_pipeline.config import CONFIG, get_storage_path, DEFAULT_NAD_VERSION
from arp_pipeline.download_utils import download_zip

DB_CONN = CONFIG["DB_CONN"]

class DownloadNADZip(luigi.Task):
    """Download the national address database from the U.S. Department of Transportation"""

    version: int = luigi.IntParameter(default=DEFAULT_NAD_VERSION)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            get_storage_path(f"nad/{self.version}/NAD_r{self.version}.zip"),
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        url = f"https://nationaladdressdata.s3.amazonaws.com/NAD_r{self.version}.zip"
        with self.output().open("wb") as f:
            f.write(download_zip(url))


class UnzipNADData(luigi.Task):
    version: int = luigi.IntParameter(default=DEFAULT_NAD_VERSION)

    def requires(self) -> DownloadNADZip:
        return DownloadNADZip(version=self.version)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            get_storage_path(f"nad/{self.version}/NAD_r{self.version}.gdb"),
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        zip_path = os.path.abspath(self.input().path)
        with ZipFile(zip_path) as feature_zip:
            os.chdir(os.path.dirname(zip_path))
            feature_zip.extractall()


class LoadNADData(luigi.Task):
    version: int = luigi.IntParameter(default=DEFAULT_NAD_VERSION)

    def requires(self) -> UnzipNADData:
        return UnzipNADData(version=self.version)

    def output(self) -> SQLAlchemyTarget:
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table="addresses.nad",
            update_id=f"create_{self.version}_nad",
        )

    def get_ogr_connection_string(self) -> str:
        url = self.output().engine.url
        base = (
            f"PG:host={url.host} dbname={url.database} active_schema=addresses_staging"
        )
        if url.username:
            base = " ".join((base, f"user={url.username}"))
        if url.password:
            base = " ".join((base, f"password={url.password}"))
        return base

    def run(self) -> None:
        with self.output().engine.connect() as conn:
            
            run_sql = lambda statement: conn.execute(text(statement))
            gdb_path = os.path.abspath(get_storage_path(f"nad/{self.version}/NAD_r{self.version}.gdb"))
            gdb_dir = os.path.dirname(gdb_path)
            ogr2ogr_cmd = (
                ogr2ogr[
                    "-overwrite",
                    f"{self.get_ogr_connection_string()}",
                    "-nlt",
                    "PROMOTE_TO_MULTI",
                    "-t_srs",
                    "EPSG:4269",  # Reproject these data into the same geometry TIGER uses
                    "-lco",
                    "GEOMETRY_NAME=the_geom",
                    gdb_path,
                ]
                .with_cwd(gdb_dir)
                .with_env(PG_USE_COPY="YES")
            )
            with conn.begin():
                run_sql("DROP SCHEMA IF EXISTS addresses_staging CASCADE;")
                run_sql("CREATE SCHEMA addresses_staging;")
            print(ogr2ogr_cmd())
            with conn.begin():
                run_sql("DROP SCHEMA IF EXISTS addresses CASCADE;")
                run_sql("CREATE SCHEMA addresses;")
                run_sql("ALTER TABLE addresses_staging.nad SET SCHEMA addresses;")
                run_sql("DROP SCHEMA IF EXISTS addresses_staging CASCADE;")
                run_sql(
                    "CREATE INDEX idx_nad_address_state on addresses.nad USING btree (state);"
                )
                self.output().touch()
