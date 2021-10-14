from geoalchemy2 import Geometry
from sqlalchemy import Column, ForeignKey, Integer, String, Table

from arp_pipeline.models import metadata

address_hud_income_limit = Table(
    "address_hud_income_limit",
    metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "address_objectid", Integer, ForeignKey("addresses.nad.objectid"), index=True
    ),
    Column(
        "fips2010",
        String(length=11),
        ForeignKey("hud.income_limits.fips2010"),
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


address_tract = Table(
    "address_tract",
    metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "address_objectid", Integer, ForeignKey("addresses.nad.objectid"), index=True
    ),
    Column("tract_id", String(length=11), index=True, nullable=False),
    Column(
        "the_geom",
        Geometry("MULTIPOINTZ", srid=4269, spatial_index=True),
        nullable=True,
    ),
    schema="lookups",
)
