import warnings
from functools import cached_property

import luigi
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import text

from arp_pipeline.config import CONFIG, DEFAULT_CENSUS_YEAR
from arp_pipeline.hud import LoadHUDData, LoadHUDFMRGeos
from arp_pipeline.models import metadata
from arp_pipeline.models.address_income import (
    get_address_hud_income_limit_for_state,
    get_address_tract_for_state,
)
from arp_pipeline.models.hud_fmr_lookup import hud_fmr_geo_lookup
from arp_pipeline.nad import LoadNADData
from arp_pipeline.tiger_national import LoadNationalData
from arp_pipeline.tiger_state import LoadStateFeatures

DB_CONN = CONFIG["DB_CONN"]


class CreateHUDCBSALookups(luigi.Task):
    fiscal_year: int = luigi.IntParameter(default=21)

    def requires(self):
        yield LoadHUDData(fiscal_year=self.fiscal_year)
        yield LoadHUDFMRGeos()

    @property
    def table(self):
        return hud_fmr_geo_lookup

    def output(self) -> SQLAlchemyTarget:
        target_table = f"lookups.{self.table.name}"
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=target_table,
            update_id=f"create_{self.fiscal_year}_{self.table.name}",
        )

    def run(self):
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            with conn.begin():
                run_sql("CREATE SCHEMA IF NOT EXISTS lookups;")
            self.table.drop(self.output().engine, checkfirst=True)
            self.table.create(self.output().engine)
            with conn.begin():
                run_sql(
                    f"""
                    insert into lookups.{self.table.name}(
                            state_usps,
                            cbsasub,
                            the_geom
                            )
                    select distinct on (income_limits.cbsasub)
                        income_limits.state_alpha,
                        income_limits.cbsasub,
                        hud.fair_market_rents.the_geom
                    from hud.income_limits
                    join hud.fair_market_rents on fair_market_rents.fmr_code = hud.income_limits.cbsasub;
                """
                )
                self.output().touch()


class CreateHUDAddressLookups(luigi.Task):

    fiscal_year: int = luigi.IntParameter(default=21)
    nad_version: int = luigi.IntParameter(default=7)
    state_usps: str = luigi.Parameter(default="OH")

    def requires(self):
        yield LoadHUDData(fiscal_year=self.fiscal_year)
        yield LoadNADData(version=self.nad_version)
        yield CreateHUDCBSALookups(fiscal_year=self.fiscal_year)

    @cached_property
    def table(self):
        return get_address_hud_income_limit_for_state(self.state_usps)

    def output(self) -> SQLAlchemyTarget:
        target_table = f"{self.table.schema}.{self.table.name}"
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=target_table,
            update_id=f"create_{self.fiscal_year}_{self.nad_version}_{self.state_usps}_hud_address_lookup",
        )

    def run(self):
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            with conn.begin():
                run_sql(f"CREATE SCHEMA IF NOT EXISTS {self.table.schema};")
            metadata.reflect(bind=self.output().engine, schema="addresses")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                metadata.reflect(bind=self.output().engine, schema="tiger")
                metadata.reflect(bind=self.output().engine, schema="hud")
            self.table.drop(self.output().engine, checkfirst=True)
            self.table.create(self.output().engine)

            with conn.begin():
                run_sql(
                    f"""
                    insert into {self.table.schema}.{self.table.name}(
                            address_objectid,
                            cbsasub,
                            the_geom
                    )
                    select
                        addresses.nad.objectid,
                        lookups.hud_fmr_geo_lookup.cbsasub,
                        addresses.nad.the_geom
                    from addresses.nad
                    join lookups.hud_fmr_geo_lookup on ST_Within(addresses.nad.the_geom, lookups.hud_fmr_geo_lookup.the_geom)
                    where addresses.nad.state = '{self.state_usps}';
                """
                )
                self.output().touch()


class CreateTractLookups(luigi.Task):
    nad_version: int = luigi.IntParameter(default=7)
    state_usps: str = luigi.Parameter(default="OH")
    census_year: int = luigi.IntParameter(default=DEFAULT_CENSUS_YEAR)

    def requires(self):
        yield LoadNADData(version=self.nad_version)
        yield LoadNationalData(year=self.census_year)
        yield LoadStateFeatures(year=self.census_year, state_usps=self.state_usps)

    @cached_property
    def table(self):
        return get_address_tract_for_state(self.state_usps)

    def output(self) -> SQLAlchemyTarget:
        target_table = f"{self.table.schema}.{self.table.name}"
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=target_table,
            update_id=f"create_{self.nad_version}_{self.state_usps}_tract_lookup",
        )

    def run(self) -> None:
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            with conn.begin():
                run_sql(f"CREATE SCHEMA IF NOT EXISTS {self.table.schema};")
            metadata.reflect(bind=self.output().engine, schema="addresses")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                metadata.reflect(bind=self.output().engine, schema="tiger")
                metadata.reflect(bind=self.output().engine, schema="hud")
            self.table.drop(self.output().engine, checkfirst=True)
            self.table.create(self.output().engine)
            with conn.begin():
                run_sql(
                    f"""
                insert into {self.table.schema}.{self.table.name}(
                    address_objectid,
                    tract_id,
                    the_geom
                )
                select
                    addresses.nad.objectid,
                    tiger_data.{self.state_usps.lower()}_tract.tract_id,
                    addresses.nad.the_geom
                from addresses.nad
                join tiger_data.{self.state_usps}_tract
                    on ST_Within(addresses.nad.the_geom, tiger_data.{self.state_usps.lower()}_tract.the_geom)
                where addresses.nad.state = '{self.state_usps}';
                """
                )
                self.output().touch()
