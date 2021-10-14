from geoalchemy2 import Geometry
from sqlalchemy import Column, Integer, String, Table

from arp_pipeline.models import metadata

fips2010_geo_lookups = Table(
    "fips2010_geo_lookups",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("state_usps", String(length=2), unique=False, index=True, nullable=False),
    Column("fips2010", String(length=10), unique=True, nullable=False, index=True),
    Column("cntyidfp", String(length=5), unique=True, nullable=True),
    Column("cosbidfp", String(length=10), unique=True, nullable=True),
    Column(
        "the_geom",
        Geometry("MULTIPOLYGON", srid=4269, spatial_index=True),
        nullable=True,
    ),
    schema="lookups",
)
