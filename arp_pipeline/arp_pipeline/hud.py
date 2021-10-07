import os

import luigi
import pandas as pd
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import text
from sqlalchemy.engine import Connection

from arp_pipeline.config import get_db_connection_string
from arp_pipeline.data_utils import clean_frame
from arp_pipeline.download_utils import download_file

DB_CONN = get_db_connection_string()
CWD = os.path.abspath(os.getcwd())


class DownloadHudIncomeLimits(luigi.Task):
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


class LoadHudData(luigi.Task):
    fiscal_year: int = luigi.IntParameter(default=21)

    def requires(self) -> DownloadHudIncomeLimits:
        return DownloadHudIncomeLimits(fiscal_year=self.fiscal_year)

    def output(self) -> SQLAlchemyTarget:
        target_table = "hud.income_limits"
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=target_table,
            update_id=f"create_{self.fiscal_year}_income_limits",
        )


    def _extract(self) -> None:
        self.data = pd.read_excel(self.input().path, dtype='object')

    def _transform(self) -> None:
        self.data = clean_frame(self.data)
        self.data['cntyidfp'] = self.data.fips2010.str.replace('99999', '')
        self.data.loc[~self.data.fips2010.str.endswith('99999'), 'cntyidfp'] = None

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
            "CREATE INDEX idx_hud_income_limits_fips2010 on hud.income_limits USING btree (fips2010);"
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
