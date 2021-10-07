import luigi
from arp_pipeline.models.fips2010_lookup import fips2010_geo_lookups
from arp_pipeline.models.address_income import address_income
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import text
from arp_pipeline.tiger_national import LoadCountyData
from arp_pipeline.hud import LoadHudData
from arp_pipeline.config import get_db_connection_string
from arp_pipeline.models import metadata

DB_CONN = get_db_connection_string()


class CreateHUDFIPSLookups(luigi.Task):
    """HUD's Income Limits data use a combination of county and county subdivison
    codes. To simplify later pipeline stages, we create a lookup table of every fip2010
    code and the associated geometry.
    """
    fiscal_year: int = luigi.IntParameter(default=21)

    def requires(self):
        pass
        # TODO: Decide if we want to depend on loading 2010 TIGER data explicitly
        # yield LoadHudData(fiscal_year=self.fiscal_year)

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
                run_sql("""
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
                """)
                run_sql("""
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
                """)


class CreateAddressLookups(luigi.Task):
    """HUD's Income Limits data use a combination of county and county subdivison
    codes. To simplify later pipeline stages, we create a lookup table of every fip2010
    code and the associated geometry.
    """
    fiscal_year: int = luigi.IntParameter(default=21)
    nad_version: int = luigi.IntParameter(default=7)
    state_usps: int = luigi.Parameter(default='OH')

    def requires(self):
        pass
        # TODO: Decide if we want to depend on loading 2010 TIGER data explicitly
        # yield LoadHudData(fiscal_year=self.fiscal_year)

    def output(self) -> SQLAlchemyTarget:
        target_table = "lookups.address_income"
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=target_table,
            update_id=f"create_{self.fiscal_year}_{self.nad_version}_address_income",
        )

    def run(self):
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            with conn.begin():
                run_sql("CREATE SCHEMA IF NOT EXISTS lookups;")
            metadata.reflect(bind=self.output().engine, schema='addresses')
            address_income.drop(self.output().engine, checkfirst=True)
            address_income.create(self.output().engine)
