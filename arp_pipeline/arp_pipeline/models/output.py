"""Final output tables that are then shared in a variety of formats."""
from sqlalchemy import Boolean, Column, Float, Integer, String, Table

from arp_pipeline.models import metadata


def get_address_income_fact_for_state(state_usps: str) -> Table:
    return Table(
        f"address_income_fact_{state_usps.lower()}",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("address_objectid", Integer, index=True),
        Column(
            "cbsasub",
            String(length=16),
            index=True,
            nullable=False,
        ),
        Column("tract_id", String(length=11), index=True, nullable=False),
        Column("does_income_qualify", Boolean, index=True, default=False),
        Column("hud_income_limit", Integer),
        Column("tract_median_renter_income", Float),
        Column(
            "state",
            String(length=2),
        ),
        Column(
            "county",
            String(length=40),
        ),
        Column(
            "inc_muni",
            String(length=100),
        ),
        Column(
            "uninc_comm",
            String(length=100),
        ),
        Column(
            "nbrhd_comm",
            String(length=100),
        ),
        Column(
            "post_comm",
            String(length=40),
        ),
        Column(
            "zip_code",
            String(length=7),
        ),
        Column(
            "plus_4",
            String(length=7),
        ),
        Column(
            "bulk_zip",
            String(length=7),
        ),
        Column(
            "bulk_plus4",
            String(length=7),
        ),
        Column(
            "stn_premod",
            String(length=15),
        ),
        Column(
            "stn_predir",
            String(length=50),
        ),
        Column(
            "stn_pretyp",
            String(length=35),
        ),
        Column(
            "stn_presep",
            String(length=20),
        ),
        Column(
            "streetname",
            String(length=60),
        ),
        Column(
            "stn_postyp",
            String(length=50),
        ),
        Column(
            "stn_posdir",
            String(length=50),
        ),
        Column(
            "stn_posmod",
            String(length=25),
        ),
        Column(
            "addnum_pre",
            String(length=15),
        ),
        Column(
            "add_number",
            Integer(),
        ),
        Column(
            "addnum_suf",
            String(length=15),
        ),
        Column(
            "unit",
            String(length=75),
        ),
        Column(
            "floor",
            String(length=75),
        ),
        Column(
            "longitude",
            Float(precision=53),
        ),
        Column(
            "latitude",
            Float(precision=53),
        ),
        schema="output",
    )
