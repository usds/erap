import os
from zipfile import ZipFile

import luigi
import pandas as pd
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import text
from sqlalchemy.engine import Connection

from arp_pipeline.config import get_db_connection_string
from arp_pipeline.data_utils import clean_frame
from arp_pipeline.download_utils import download_file, download_zip
from arp_pipeline.tiger_utils import get_shp2pgsql_cmd


DB_CONN = get_db_connection_string()
CWD = os.path.abspath(os.getcwd())


class DownloadHUDIncomeLimits(luigi.Task):
    """Download the xlsx of all income limit data for section 8 housing."""

    fiscal_year: int = luigi.IntParameter(default=21)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            os.path.join(
                CWD,
                f"data/hud/il/il{self.fiscal_year}/Section8-{self.fiscal_year}.xlsx",
            ),
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        url = f"https://www.huduser.gov/portal/datasets/il/il{self.fiscal_year}/Section8-FY{self.fiscal_year}.xlsx"
        with self.output().open("wb") as f:
            f.write(download_file(url).content)


class DownloadHUDFMRGeos(luigi.Task):
    """The income limits data is keyed to HUD Fair Market Rents, which are officially hosted on
    arcgis's open data platform:

    https://hudgis-hud.opendata.arcgis.com/datasets/HUD::fair-market-rents/about
    """

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            os.path.join(
                CWD,
                "data/hud/fmr/Fair_Market_Rents.zip"
            ),
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        url = ("https://opendata.arcgis.com/api/v3/datasets/"
               "12d2516901f947b5bb4da4e780e35f07_0/downloads/data?format=shp&spatialRefId=4326")
        with self.output().open("wb") as f:
            f.write(download_zip(url))


class UnzipHUDFMRGeos(luigi.Task):
    def requires(self) -> DownloadHUDFMRGeos:
        return DownloadHUDFMRGeos()

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            os.path.join(
                CWD,
                "data/hud/fmr/Fair_Market_Rents.dbf"
            )
        )

    def run(self) -> None:
        zip_path = os.path.abspath(self.input().path)
        with ZipFile(zip_path) as fmr_zip:
            os.chdir(os.path.dirname(zip_path))
            fmr_zip.extractall()



class LoadHUDData(luigi.Task):
    fiscal_year: int = luigi.IntParameter(default=21)

    def requires(self) -> DownloadHUDIncomeLimits:
        return DownloadHUDIncomeLimits(fiscal_year=self.fiscal_year)

    def output(self) -> SQLAlchemyTarget:
        target_table = "hud.income_limits"
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=target_table,
            update_id=f"create_{self.fiscal_year}_income_limits",
        )

    def _extract(self) -> None:
        self.data = pd.read_excel(self.input().path, dtype="object")

    def _transform(self) -> None:
        self.data = clean_frame(self.data)
        self.data["cntyidfp"] = self.data.fips2010.str.replace("99999", "")
        self.data.loc[~self.data.fips2010.str.endswith("99999"), "cntyidfp"] = None

    def _load(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql("CREATE SCHEMA IF NOT EXISTS hud;")
        self.data.to_sql(
            con=connection,
            name="income_limits",
            schema="hud",
            if_exists="replace",
            index=False,
        )
        run_sql(
            "CREATE  UNIQUE INDEX idx_hud_income_limits_fips2010 on hud.income_limits USING btree (fips2010);"
        )
        run_sql(
            "CREATE INDEX idx_hud_income_limits_state on hud.income_limits USING btree (state);"
        )
        run_sql(
            "CREATE INDEX idx_hud_income_limits_cntyidfp on hud.income_limits USING btree (cntyidfp);"
        )

    def run(self) -> None:
        self._extract()
        self._transform()
        with self.output().engine.connect() as conn:
            with conn.begin():
                self._load(conn)
                self.output().touch()


class LoadHUDFMRGeos(luigi.Task):

    target_table = "hud.fair_market_rents"

    @property
    def table_name(self) -> str:
        return self.target_table.split('.')[-1]

    def requires(self) -> UnzipHUDFMRGeos:
        return UnzipHUDFMRGeos()

    def output(self) -> SQLAlchemyTarget:
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=self.target_table,
            update_id=f"create_hud_fair_market_rents",
        )

    def run(self) -> None:
        dbf_file_path = os.path.abspath(self.input().path)
        dbf_file_dir = os.path.dirname(dbf_file_path)

        cmd_chain = get_shp2pgsql_cmd(DB_CONN, dbf_file_path, self.target_table, srid='4326')
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            with conn.begin():
                run_sql("CREATE SCHEMA IF NOT EXISTS hud;")
                run_sql(f"DROP TABLE IF EXISTS hud.{self.table_name} CASCADE;")
            cmd_chain.with_cwd(dbf_file_dir)()
            with conn.begin():
                run_sql(f"ALTER TABLE {self.target_table} ALTER COLUMN the_geom TYPE geometry(MultiPolygon, 4269) USING ST_SetSRID(the_geom, 4269);")
                run_sql(f"CREATE INDEX hud_{self.table_name}_the_geom_gist ON {self.target_table} USING gist(the_geom);")
                run_sql(f"CREATE INDEX idx_{self.table_name}_fmr_code ON {self.target_table} USING btree (fmr_code);")
                self.output().touch()
