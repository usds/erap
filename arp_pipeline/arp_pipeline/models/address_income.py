from geoalchemy2 import Geometry
from sqlalchemy import Column, ForeignKey, Integer, String, Table

from arp_pipeline.models import metadata


def get_address_hud_income_limit_for_state(state_usps: str) -> Table:
    return Table(
        f"address_hud_income_limit_{state_usps.lower()}",
        metadata,
        Column("id", Integer, primary_key=True),
        Column(
            "address_objectid",
            Integer,
            ForeignKey("addresses.nad.objectid"),
            index=True,
        ),
        Column(
            "cbsasub",
            String(length=16),
            index=True,
            nullable=False,
        ),
        Column(
            "the_geom",
            Geometry("MULTIPOINTZ", srid=4269, spatial_index=True),
            nullable=True,
        ),
        schema="lookups",
    )


def get_address_tract_for_state(state_usps: str) -> Table:
    return Table(
        f"address_tract_{state_usps.lower()}",
        metadata,
        Column("id", Integer, primary_key=True),
        Column(
            "address_objectid",
            Integer,
            ForeignKey("addresses.nad.objectid"),
            index=True,
        ),
        Column("tract_id", String(length=11), index=True, nullable=False),
        Column(
            "the_geom",
            Geometry("MULTIPOINTZ", srid=4269, spatial_index=True),
            nullable=True,
        ),
        schema="lookups",
    )
