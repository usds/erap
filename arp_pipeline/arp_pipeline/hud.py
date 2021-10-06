import os

import luigi
import pandas as pd
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import text

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

    def run(self) -> None:
        limits = clean_frame(pd.read_excel(self.input().path))
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            with conn.begin():
                run_sql("CREATE SCHEMA IF NOT EXISTS hud;")
                limits.to_sql(
                    con=conn,
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
