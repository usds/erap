from sqlalchemy import Column, Integer, String, ForeignKey, Table
from geoalchemy2 import Geometry
from arp_pipeline.models import metadata


address_income = Table(
    "address_income",
    metadata,
    Column('id', Integer, primary_key=True),
    Column('address_objectid', Integer, ForeignKey('addresses.nad.objectid')),
    Column('inc_muni', String(length=100), nullable=True),
    Column('uninc_comm', String(length=100), nullable=True),
    Column('nbrhd_comm', String(length=100), nullable=True),
    Column('state_usps', String(length=2), unique=False, index=True, nullable=False),
    Column('cntyidfp', String(length=5), nullable=True),
    Column('cosbidfp', String(length=10), nullable=True),
    Column('zip_code', String(length=5), nullable=False),
    Column('plus_4', String(length=4), nullable=True),
    Column('addnum_pre', String(length=15), nullable=True),
    Column('add_number', Integer, nullable=True),
    Column('addnum_psuf', String(length=15), nullable=True),
    Column('plus_4', String(length=4), nullable=False),
    Column('stn_premod', String(length=15), nullable=True),
    Column('stn_predir', String(length=50), nullable=True),
    Column('stn_pretyp', String(length=35), nullable=True),
    Column('stn_presep', String(length=20), nullable=True),
    Column('streetname', String(length=60), nullable=True),
    Column('stn_postyp', String(length=50), nullable=True),
    Column('stn_posdir', String(length=50), nullable=True),
    Column('stn_posmod', String(length=25), nullable=True),
    Column('unit', String(length=75), nullable=True),
    Column('median_income', Integer, nullable=True),
    Column('l80_1', Integer, nullable=True),
    Column('l50_1', Integer, nullable=True),
    Column('the_geom', Geometry('MULTIPOLYGON', srid=4269, spatial_index=True), nullable=True),
    schema='lookups',
)
