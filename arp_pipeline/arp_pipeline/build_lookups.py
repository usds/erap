import warnings

import luigi
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import text

from functools import cached_property
from arp_pipeline.config import get_db_connection_string
from arp_pipeline.hud import LoadHudData
from arp_pipeline.models import metadata
from arp_pipeline.models.address_income import get_address_hud_income_limit_for_state,  get_address_tract_for_state
from arp_pipeline.models.fips2010_lookup import fips2010_geo_lookups
from arp_pipeline.nad import LoadNADData

DB_CONN = get_db_connection_string()


class CreateHUDFIPSLookups(luigi.Task):
    """HUD's Income Limits data use a combination of county and county subdivison
    codes. To simplify later pipeline stages, we create a lookup table of every fip2010
    code and the associated geometry.
    """

    fiscal_year: int = luigi.IntParameter(default=21)

    def requires(self):
        yield LoadHudData(fiscal_year=self.fiscal_year)

    def output(self) -> SQLAlchemyTarget:
        target_table = "lookups.fips2010_geo_lookups"
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=target_table,
            update_id=f"create_{self.fiscal_year}_fips2010_geo_lookups",
        )

    def run(self):
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            with conn.begin():
                run_sql("CREATE SCHEMA IF NOT EXISTS lookups;")
            fips2010_geo_lookups.drop(self.output().engine, checkfirst=True)
            fips2010_geo_lookups.create(self.output().engine)
            with conn.begin():
                run_sql(
                    """
                    insert into lookups.fips2010_geo_lookups(
                            state_usps,
                            fips2010,
                            cntyidfp,
                            the_geom
                            )
                    select
                        income_limits.state_alpha,
                        income_limits.fips2010,
                        income_limits.cntyidfp,
                        tiger.county.the_geom
                    from hud.income_limits
                    join tiger.county on income_limits.cntyidfp = tiger.county.cntyidfp
                    where income_limits.cntyidfp is not null;
                """
                )
                run_sql(
                    """
                    insert into lookups.fips2010_geo_lookups(
                            state_usps,
                            fips2010,
                            cosbidfp,
                            the_geom
                            )
                    select
                        income_limits.state_alpha,
                        income_limits.fips2010,
                        income_limits.cntyidfp,
                        tiger.cousub.the_geom
                    from hud.income_limits
                    join tiger.cousub on hud.income_limits.fips2010 = tiger.cousub.cosbidfp
                    where income_limits.cntyidfp is null;
                """
                )
                self.output().touch()


class CreateHUDAddressLookups(luigi.Task):
    """HUD's Income Limits data use a combination of county and county subdivison
    codes. To simplify later pipeline stages, we create a lookup table of every fip2010
    code and the associated geometry.
    """

    fiscal_year: int = luigi.IntParameter(default=21)
    nad_version: int = luigi.IntParameter(default=7)
    state_usps: str = luigi.Parameter(default="OH")

    def requires(self):
        yield LoadHudData(fiscal_year=self.fiscal_year)
        yield LoadNADData(version=self.nad_version)

    @cached_property
    def table(self):
        return get_address_hud_income_limit_for_state(self.state_usps)

    def output(self) -> SQLAlchemyTarget:
        target_table = f"{self.table.schema}_{self.table.name}"
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
                            fips2010,
                            the_geom
                    )
                    select
                        addresses.nad.objectid,
                        lookups.fips2010_geo_lookups.fips2010,
                        addresses.nad.the_geom
                    from addresses.nad
                    join lookups.fips2010_geo_lookups on ST_Within(addresses.nad.the_geom, lookups.fips2010_geo_lookups.the_geom)
                    where addresses.nad.state = '{self.state_usps}';
                """
                )
                self.output().touch()


class CreateTractLookups(luigi.Task):
    nad_version: int = luigi.IntParameter(default=7)
    state_usps: str = luigi.Parameter(default="OH")

    # def requires(self):
    #     # NOTE: This should depend on LoadStateTracts but figuring
    #     # out which year to ask for is complicated, so I'm punting that
    #     yield LoadNADData(version=self.nad_version)

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
