from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, String, Table

from arp_pipeline.models import metadata

hud_fmr_geo_lookup = Table(
    "hud_fmr_geo_lookup",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("state_usps", String(length=2), unique=False, index=True, nullable=False),
    Column("cbsasub", String(length=16), unique=True, nullable=False, index=True),
    Column(
        "the_geom",
        Geometry("MULTIPOLYGON", srid=4269, spatial_index=True),
        nullable=True,
    ),
    schema="lookups",
)
