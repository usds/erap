"""Generate lookup tables for addresses and geographies."""
import os
import pathlib
import shutil
import tempfile
import warnings
from functools import cached_property
from typing import List

import luigi
import pandas as pd
from luigi.contrib.sqla import SQLAlchemyTarget
from plumbum.cmd import pg_dump
from sqlalchemy import text

from arp_pipeline.build_lookups import CreateHUDAddressLookups, CreateTractLookups
from arp_pipeline.census import LoadTractLevelACSData
from arp_pipeline.config import CONFIG, DEFAULT_CENSUS_YEAR, get_output_path
from arp_pipeline.hud import LoadHUDData
from arp_pipeline.models import metadata
from arp_pipeline.models.output import (
    base_income_fact_table,
    get_address_income_fact_for_state,
)

DB_CONN = CONFIG["DB_CONN"]


class CreateBaseIncomeFactTable(luigi.Task):
    priority = 90

    @property
    def table(self):
        return base_income_fact_table

    def output(self) -> SQLAlchemyTarget:
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=f"{self.table.schema}.{self.table.name}",
            update_id="create_base_address_income_table",
        )

    def run(self):
        db_name = self.output().connection_string.split("/")[-1]
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            with conn.begin():
                run_sql(f"CREATE SCHEMA IF NOT EXISTS {self.table.schema}")
                run_sql(
                    f'ALTER DATABASE {db_name} SET search_path TO "$user",public,tiger,output;'
                )
            self.table.create(self.output().engine, checkfirst=True)
            with conn.begin():
                self.output().touch()


class CreateAddressIncomeFact(luigi.Task):
    state_usps: str = luigi.Parameter(default="OH")
    census_year: int = luigi.IntParameter(default=DEFAULT_CENSUS_YEAR)

    def requires(self):
        yield CreateTractLookups(state_usps=self.state_usps)
        yield CreateHUDAddressLookups(state_usps=self.state_usps)
        yield LoadTractLevelACSData(year=self.census_year)
        yield LoadHUDData()
        yield CreateBaseIncomeFactTable()

    @cached_property
    def table(self):
        return get_address_income_fact_for_state(self.state_usps)

    def output(self) -> SQLAlchemyTarget:
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=f"{self.table.schema}.{self.table.name}",
            update_id=f"create_{self.state_usps}_address_income_lookups",
        )

    def run(self):
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            with conn.begin():
                run_sql(f"CREATE SCHEMA IF NOT EXISTS {self.table.schema}")
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                metadata.reflect(bind=self.output().engine, schema="output")

            self.table.drop(self.output().engine, checkfirst=True)
            self.table.create(self.output().engine)
            with conn.begin():
                run_sql(
                    f"""
                    insert into {self.table.schema}.{self.table.name} (
                        address_objectid,
                        cbsasub,
                        tract_id,
                        does_income_qualify,
                        hud_income_limit,
                        tract_median_renter_income,
                        state,
                        county,
                        inc_muni,
                        uninc_comm,
                        nbrhd_comm,
                        post_comm,
                        zip_code,
                        plus_4,
                        bulk_zip,
                        bulk_plus4,
                        stn_premod,
                        stn_predir,
                        stn_pretyp,
                        stn_presep,
                        streetname,
                        stn_postyp,
                        stn_posdir,
                        stn_posmod,
                        addnum_pre,
                        add_number,
                        addnum_suf,
                        unit,
                        floor,
                        longitude,
                        latitude
                    )
                    select
                        distinct on (addresses.nad.objectid)
                        addresses.nad.objectid,
                        addr_hud_il.cbsasub,
                        addr_tract.tract_id,
                        tract_data."B25119_003E" < l80_1 as does_income_qualify,
                        l80_1,
                        tract_data."B25119_003E",
                        nad.state,
                        nad.county,
                        nad.inc_muni,
                        nad.uninc_comm,
                        nad.nbrhd_comm,
                        nad.post_comm,
                        nad.zip_code,
                        nad.plus_4,
                        nad.bulk_zip,
                        nad.bulk_plus4,
                        nad.stn_premod,
                        nad.stn_predir,
                        nad.stn_pretyp,
                        nad.stn_presep,
                        nad.streetname,
                        nad.stn_postyp,
                        nad.stn_posdir,
                        nad.stn_posmod,
                        nad.addnum_pre,
                        nad.add_number,
                        nad.addnum_suf,
                        nad.unit,
                        nad.floor,
                        nad.longitude,
                        nad.latitude

                    from addresses.nad
                    join lookups.address_tract_{self.state_usps.lower()} as addr_tract on addr_tract.address_objectid = addresses.nad.objectid
                    join lookups.address_hud_income_limit_{self.state_usps.lower()}  addr_hud_il on addr_hud_il.address_objectid = addresses.nad.objectid
                    join hud.income_limits as hud_il on addr_hud_il.cbsasub = hud_il.cbsasub
                    join census.acs_tract_data as tract_data on tract_data.tract_id = addr_tract.tract_id
                    where nad.state='{self.state_usps.upper()}';
                """
                )
                self.output().touch()


class CreateAddressIncomeParquet(luigi.Task):
    state_usps: str = luigi.Parameter(default="OH")

    def requires(self) -> CreateAddressIncomeFact:
        return CreateAddressIncomeFact(state_usps=self.state_usps)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            get_output_path(
                f"2019/{self.state_usps}/address-income-{self.state_usps.lower()}.parquet"
            ),
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        with self.input().engine.connect() as conn:
            query = f"select * from {self.input().target_table};"
            frame = pd.read_sql(query, conn)
            with self.output().open("wb") as f:
                frame.to_parquet(f, index=False)


class CreateStateAddressIncomePGDump(luigi.Task):
    state_usps: str = luigi.Parameter(default="OH")

    @property
    def output_path(self) -> str:
        return get_output_path(
            f"2019/{self.state_usps}/address-income-{self.state_usps.lower()}.sql"
        )

    def requires(self) -> CreateAddressIncomeFact:
        return CreateAddressIncomeFact(state_usps=self.state_usps)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            self.output_path,
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_out_path = os.path.join(
                tmpdirname, f"adress-income-{self.state_usps.lower()}.sql"
            )
            pg_dump_cmd = pg_dump[
                "-Fc",
                "-f",
                tmp_out_path,
                "--table",
                self.input().target_table,
                DB_CONN,
            ]
            pg_dump_cmd()
            pathlib.Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.move(tmp_out_path, self.output_path)


class CreateOverallAddressIncomePGDump(luigi.Task):

    @property
    def output_path(self) -> str:
        return get_output_path(
            f"2019/national/address-income.sql"
        )

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            self.output_path,
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdirname:
            tmp_out_path = os.path.join(
                tmpdirname, f"adress-income-national.sql"
            )
            pg_dump_cmd = pg_dump[
                "-Fc",
                "-f",
                tmp_out_path,
                "--table",
                "public.address_income_fact",
                DB_CONN,
            ]
            pg_dump_cmd()
            pathlib.Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.move(tmp_out_path, self.output_path)


class CreateAddressIncomeCSV(luigi.Task):
    state_usps: str = luigi.Parameter(default="OH")

    def requires(self) -> CreateAddressIncomeParquet:
        return CreateAddressIncomeParquet(state_usps=self.state_usps)

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            get_output_path(
                f"2019/{self.state_usps}/address-income-{self.state_usps.lower()}.csv.zip",
            ),
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        with self.input().open() as f:
            frame = pd.read_parquet(f)
        with self.output().open("wb") as f:
            frame.to_csv(f, index=False, compression="zip")


class CreateAllOutputForState(luigi.WrapperTask):
    state_usps: str = luigi.Parameter()

    def requires(self):
        yield CreateAddressIncomeParquet(state_usps=self.state_usps)
        yield CreateStateAddressIncomePGDump(state_usps=self.state_usps)
        yield CreateAddressIncomeCSV(state_usps=self.state_usps)


class CreateAllOutputForStates(luigi.WrapperTask):
    state_uspses: List[str] = luigi.ListParameter()

    def requires(self):
        for state_usps in self.state_uspses:
            yield CreateAllOutputForState(state_usps=state_usps)
