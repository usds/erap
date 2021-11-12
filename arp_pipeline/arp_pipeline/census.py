from typing import List

import censusdata
import luigi
import numpy as np
import pandas as pd
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import String, text
from sqlalchemy.engine import Connection

from arp_pipeline.config import CONFIG

DB_CONN = CONFIG["DB_CONN"]


class LoadTractLevelACSData(luigi.Task):
    """Load ACS data for a set of variables."""

    acs_variables: List[str] = luigi.ListParameter(default=["B25119_003E", 'B19013_001E'])
    year: int = luigi.IntParameter(default=2019)

    target_table = "acs_tract_data"

    def output(self) -> SQLAlchemyTarget:
        variables = "_".join(self.acs_variables)
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=f"census.{self.target_table}",
            update_id=f"create_{self.year}_census_acs_{variables}",
        )

    @staticmethod
    def _fips_from_censusdata_censusgeo(censusgeo: censusdata.censusgeo) -> str:
        """Create a FIPS code from the proprietary censusgeo index."""
        # Thank you justice40 team
        fips = "".join([value for (key, value) in censusgeo.params()])
        return fips

    def _extract(self) -> None:
        state_data = []
        state_geos = censusdata.geographies(
            censusdata.censusgeo([("state", "*")]), "acs5", self.year
        )
        state_codes = (geo.params()[0][-1] for geo in state_geos.values())
        for state_code in state_codes:
            state_data.append(
                censusdata.download(
                    "acs5",
                    self.year,
                    censusdata.censusgeo([("state", state_code), ("tract", "*")]),
                    list(self.acs_variables),
                )
            )
        self.data = pd.concat(state_data)

    def _transform(self) -> None:
        NULL_SIGNALS = [-666666666, -333333333, -222222222]
        self.data["tract_id"] = self.data.index.to_series().apply(
            self._fips_from_censusdata_censusgeo
        )
        self.data.replace(to_replace=NULL_SIGNALS, value=np.nan, inplace=True)

    def _load(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql("CREATE SCHEMA IF NOT EXISTS census;")
        self.data.to_sql(
            con=connection,
            name=self.target_table,
            schema="census",
            if_exists="replace",
            index=False,
            dtype={"tract_id": String(length=11)},
        )

    def run(self) -> None:
        self._extract()
        self._transform()
        with self.output().engine.connect() as conn:
            with conn.begin():
                self._load(conn)
                self.output().touch()
