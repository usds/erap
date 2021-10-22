import os.path
from abc import ABC, ABCMeta, abstractmethod
from functools import cached_property
from typing import Generator, List
from zipfile import ZipFile

import luigi
from geoalchemy2 import Geometry  # noqa
from luigi.contrib.sqla import SQLAlchemyTarget
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, Engine

from arp_pipeline.config import get_db_connection_string, get_storage_path
from arp_pipeline.download_utils import download_zip
from arp_pipeline.tiger_national import LoadCountyData, LoadNationalData
from arp_pipeline.tiger_utils import (
    Shp2PGSqlMode,
    create_indexes_and_vacuum,
    get_shp2pgsql_cmd,
    run_raw_sql,
    staging_schema,
)

DB_CONN = get_db_connection_string()


class DownloadStateLevelLocalData(luigi.Task):
    year: int = luigi.IntParameter(default=2019)
    state_code: str = luigi.Parameter()
    county_code: str = luigi.Parameter(default="")
    feature_name: str = luigi.Parameter()
    retry_count = 5

    @property
    def path_name(self) -> str:
        # Handle the weird tabblock10 path
        if self.feature_name.endswith("10"):
            return self.feature_name[:-2]
        return self.feature_name

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            os.path.join(
                get_storage_path(),
                f"tiger/{self.year}/state/{self.state_code}/{self.feature_name}/{self.file_name}",
            ),
            format=luigi.format.Nop,
        )

    @property
    def file_name(self) -> str:
        return f"tl_{self.year}_{self.state_code}{self.county_code}_{self.feature_name.lower()}.zip"

    def run(self) -> None:
        url = (
            f"https://www2.census.gov/geo/tiger/"
            f"TIGER{self.year}/{self.path_name.upper()}/{self.file_name}"
        )
        with self.output().open("wb") as f:
            f.write(download_zip(url))


class UnzipStateLevelTigerData(luigi.Task):
    year = luigi.IntParameter(default=2019)
    state_code = luigi.Parameter()
    county_code: str = luigi.Parameter(default="")
    feature_name = luigi.Parameter()

    def requires(self) -> DownloadStateLevelLocalData:
        return DownloadStateLevelLocalData(
            year=self.year,
            state_code=self.state_code,
            county_code=self.county_code,
            feature_name=self.feature_name,
        )

    def output(self) -> luigi.LocalTarget:
        return luigi.LocalTarget(
            os.path.join(
                get_storage_path(),
                (
                    f"tiger/{self.year}/state/{self.state_code}/{self.feature_name}/"
                    f"tl_{self.year}_{self.state_code}{self.county_code}_{self.feature_name.lower()}.dbf"
                ),
            ),
            format=luigi.format.Nop,
        )

    def run(self) -> None:
        zip_path = os.path.abspath(self.input().path)
        with ZipFile(zip_path) as feature_zip:
            os.chdir(os.path.dirname(zip_path))
            feature_zip.extractall()


class LoadStateFeature(ABC):
    year = luigi.IntParameter(default=2019)
    state_usps = luigi.Parameter()
    resources = {"max_workers": 1}

    @property
    @abstractmethod
    def feature_name(self) -> str:
        """The name of this feature (e.g., place, cousub)"""

    @cached_property
    def state_code(self):
        with self.output().engine.connect() as conn:
            res = conn.execute(
                text(
                    "select statefp from tiger_data.state_all where stusps=:usps"
                ).params(usps=self.state_usps)
            )
        return res.fetchone()[0]

    @property
    def table_name(self):
        return f"{self.state_usps}_{self.feature_name}".lower()

    def requires(self) -> LoadNationalData:
        return LoadNationalData(year=self.year)

    def output(self) -> SQLAlchemyTarget:
        target_table = f"tiger_data.{self.table_name}"
        return SQLAlchemyTarget(
            connection_string=DB_CONN,
            target_table=target_table,
            update_id=f"create_{self.year}_{self.state_usps.lower()}_{self.feature_name.lower()}",
        )

    @abstractmethod
    def create_table(self, connection: Connection) -> None:
        """Any statements required to create the target table"""

    @abstractmethod
    def finalized_table(self, connection: Connection):
        """Any statements for creating constrants, renames, or indexes"""

    def vacuum(self, engine: Engine) -> None:
        run_raw_sql(engine, f"VACUUM ANALYZE tiger_data.{self.table_name}")

    def fixup_staging_schema(self, connection: Connection):
        """Make the tiger_staging.TABLE_NAME version of a table look like the tiger_data version

        `loader_load_staged_data` can fail if the colunms for a table in `tiger_staging` don't match
        the columns for the target table in `tiger_data`. Use SQLAlchemy inspection to drop any novel
        columns in the staging data, and then create NULL-filled columns for any missing data in the
        staging data. This should be called immediately before `loader_load_staged_data`.
        """
        run_sql = lambda statement: run_raw_sql(connection.engine, statement)
        insp = inspect(connection.engine)
        staging_cols = insp.get_columns(self.table_name, "tiger_staging")
        data_cols = insp.get_columns(self.table_name, "tiger_data")
        staging_not_in_data = set(col["name"] for col in staging_cols) - set(
            col["name"] for col in data_cols
        )
        data_not_in_staging = set(col["name"] for col in data_cols) - set(
            col["name"] for col in staging_cols
        )
        for col_name in staging_not_in_data:
            run_sql(
                f"ALTER TABLE tiger_staging.{self.table_name} DROP COLUMN {col_name} CASCADE;"
            )

        for col in data_cols:
            if col["name"] in data_not_in_staging:
                col_type = col["type"].dialect_impl(connection.engine.dialect).compile()
                run_sql(
                    f"ALTER TABLE tiger_staging.{self.table_name} ADD COLUMN {col['name']} {col_type} DEFAULT NULL;"
                )

    def run(self) -> Generator:
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            unzipped_data = yield UnzipStateLevelTigerData(
                year=self.year,
                state_code=self.state_code,
                feature_name=self.feature_name,
            )
            dbf_file_path = os.path.abspath(unzipped_data.path)
            dbf_file_dir = os.path.dirname(dbf_file_path)
            table_name = self.table_name
            with conn.begin():
                self.create_table(conn)
            with staging_schema(conn, self.feature_name):
                get_shp2pgsql_cmd(
                    DB_CONN,
                    dbf_file_name=dbf_file_path,
                    table_name=f"tiger_staging.{table_name}",
                ).with_cwd(dbf_file_dir)()
                self.finalized_table(conn)
                self.vacuum(self.output().engine)
                with conn.begin():
                    self.output().touch()


class LoadCountyFeature(LoadStateFeature, metaclass=ABCMeta):
    def requires(self) -> LoadCountyData:
        return LoadCountyData(year=self.year)

    @cached_property
    def county_codes(self) -> List[str]:
        with self.output().engine.connect() as conn:
            res = conn.execute(
                text(
                    "select countyfp from tiger_data.county_all where statefp=:statefp "
                ).params(statefp=self.state_code)
            )
            return [row[0] for row in res]

    def run(self) -> Generator:
        with self.output().engine.connect() as conn:
            run_sql = lambda statement: conn.execute(text(statement))
            table_name = self.table_name
            with conn.begin():
                self.create_table(conn)
            with staging_schema(conn, self.feature_name):
                for idx, county_code in enumerate(self.county_codes):
                    unzipped_data = yield UnzipStateLevelTigerData(
                        year=self.year,
                        state_code=self.state_code,
                        county_code=county_code,
                        feature_name=self.feature_name,
                    )
                    dbf_file_path = os.path.abspath(unzipped_data.path)
                    dbf_file_dir = os.path.dirname(dbf_file_path)
                    if idx == 0:
                        mode = Shp2PGSqlMode.CREATE
                    else:
                        mode = Shp2PGSqlMode.APPEND
                    get_shp2pgsql_cmd(
                        DB_CONN,
                        dbf_file_name=dbf_file_path,
                        table_name=f"tiger_staging.{table_name}",
                        mode=mode,
                    ).with_cwd(dbf_file_dir)()
                with conn.begin():
                    self.finalized_table(conn)
                    run_sql(
                        f"ALTER TABLE tiger_data.{self.table_name} ADD CONSTRAINT chk_statefp CHECK (statefp = '{self.state_code}');"
                    )
                self.vacuum(self.output().engine)
                with conn.begin():
                    self.output().touch()


class LoadStatePlaceFeature(LoadStateFeature, luigi.Task):
    feature_name = "place"

    def create_table(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql(f"DROP TABLE IF EXISTS tiger_data.{self.table_name};")
        run_sql(
            f"""CREATE TABLE tiger_data.{self.table_name}(
                CONSTRAINT pk_{self.table_name} PRIMARY KEY (plcidfp)
            ) INHERITS(tiger.place);"""
        )

    def finalized_table(self, connection: Connection):
        run_sql = lambda statement: connection.execute(text(statement))
        with connection.begin():
            run_sql(
                f"ALTER TABLE tiger_staging.{self.table_name} RENAME geoid TO plcidfp;"
            )
        self.fixup_staging_schema(connection)
        with connection.begin():
            run_sql(
                f"SELECT loader_load_staged_data(lower('{self.table_name}'), lower('{self.table_name}'));"
            )
            run_sql(
                f"ALTER TABLE tiger_data.{self.table_name} ADD CONSTRAINT uidx_{self.table_name}_gid UNIQUE (gid);"
            )
            run_sql(
                f"CREATE INDEX idx_{self.table_name}_soundex_name ON tiger_data.{self.table_name} USING btree (soundex(name));"
            )
            run_sql(
                f"CREATE INDEX tiger_data_{self.table_name}_the_geom_gist ON tiger_data.{self.table_name} USING gist(the_geom);"
            )
            run_sql(
                f"ALTER TABLE tiger_data.{self.table_name} ADD CONSTRAINT chk_statefp CHECK (statefp = '{self.state_code}');"
            )

    def vacuum(self, engine: Engine) -> None:
        pass


class LoadStateCountySubdivisions(LoadStateFeature, luigi.Task):
    feature_name = "cousub"

    def create_table(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql(f"DROP TABLE IF EXISTS tiger_data.{self.table_name};")
        run_sql(
            f"""
            CREATE TABLE tiger_data.{self.table_name}(
                CONSTRAINT pk_{self.table_name} PRIMARY KEY (cosbidfp),
                CONSTRAINT uidx_{self.state_usps}_cousub_gid UNIQUE (gid))
            INHERITS(tiger.cousub);
            """
        )

    def finalized_table(self, connection: Connection):
        run_sql = lambda statement: connection.execute(text(statement))
        with connection.begin():
            run_sql(
                f"ALTER TABLE tiger_staging.{self.table_name} RENAME geoid TO cosbidfp;"
            )
        self.fixup_staging_schema(connection)
        with connection.begin():
            run_sql(
                f"SELECT loader_load_staged_data(lower('{self.table_name}'), lower('{self.table_name}'));"
            )
            run_sql(
                f"ALTER TABLE tiger_data.{self.table_name} ADD CONSTRAINT chk_statefp CHECK (statefp = '{self.state_code}');"
            )


class LoadStateTracts(LoadStateFeature, luigi.Task):
    feature_name = "tract"

    def create_table(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql(f"DROP TABLE IF EXISTS tiger_data.{self.table_name};")
        run_sql(
            f"""
            CREATE TABLE tiger_data.{self.table_name}(
                 CONSTRAINT pk_{self.table_name} PRIMARY KEY (tract_id)
        ) INHERITS(tiger.tract);
        """
        )

    def finalized_table(self, connection: Connection):
        run_sql = lambda statement: connection.execute(text(statement))
        with connection.begin():
            run_sql(
                f"ALTER TABLE tiger_staging.{self.table_name} RENAME geoid TO tract_id"
            )
        self.fixup_staging_schema(connection)
        with connection.begin():

            run_sql(
                f"SELECT loader_load_staged_data(lower('{self.table_name}'), lower('{self.table_name}'));"
            )
            run_sql(
                f"CREATE INDEX tiger_data_{self.table_name}_the_geom_gist ON tiger_data.{self.table_name} USING gist(the_geom);"
            )
            run_sql(
                f"ALTER TABLE tiger_data.{self.table_name} ADD CONSTRAINT chk_statefp CHECK (statefp = '{self.state_code}');"
            )


class LoadTabBlocks10(LoadStateFeature, luigi.Task):
    feature_name = "tabblock10"

    @property
    def table_name(self):
        return f"{self.state_usps}_tabblock".lower()

    def create_table(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql(f"DROP TABLE IF EXISTS tiger_data.{self.table_name};")
        run_sql(
            f"""CREATE TABLE tiger_data.{self.table_name}(
                CONSTRAINT pk_{self.table_name} PRIMARY KEY (tabblock_id)
            ) INHERITS(tiger.tabblock);"""
        )

    def finalized_table(self, connection: Connection):
        run_sql = lambda statement: connection.execute(text(statement))
        with connection.begin():
            run_sql(
                f"ALTER TABLE tiger_staging.{self.table_name} RENAME geoid10 TO tabblock_id;"
            )
        self.fixup_staging_schema(connection)

        with connection.begin():
            run_sql(
                f"SELECT loader_load_staged_data(lower('{self.table_name}'), lower('{self.table_name}'));"
            )
            run_sql(
                f"ALTER TABLE tiger_data.{self.table_name} ADD CONSTRAINT chk_statefp CHECK (statefp = '{self.state_code}');"
            )
            run_sql(
                f"CREATE INDEX tiger_data_{self.table_name}_the_geom_gist ON tiger_data.{self.table_name} USING gist(the_geom);"
            )


class LoadTabBlocks20(LoadStateFeature, luigi.Task):
    feature_name = "tabblock20"

    def create_table(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql(f"DROP TABLE IF EXISTS tiger_data.{self.table_name};")
        run_sql(
            f"""CREATE TABLE tiger_data.{self.table_name}(
                CONSTRAINT pk_{self.table_name} PRIMARY KEY (geoid)
            ) INHERITS(tiger.tabblock20);"""
        )

    def finalized_table(self, connection: Connection):
        run_sql = lambda statement: connection.execute(text(statement))
        self.fixup_staging_schema(connection)
        with connection.begin():
            run_sql(
                f"SELECT loader_load_staged_data(lower('{self.table_name}'), lower('{self.table_name}'));"
            )
            run_sql(
                f"ALTER TABLE tiger_data.{self.table_name} ADD CONSTRAINT chk_statefp CHECK (statefp = '{self.state_code}');"
            )
            run_sql(
                f"CREATE INDEX tiger_data_{self.table_name}_the_geom_gist ON tiger_data.{self.table_name} USING gist(the_geom);"
            )


class LoadBlockGroups(LoadStateFeature, luigi.Task):
    feature_name = "bg"

    def create_table(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql(f"DROP TABLE IF EXISTS tiger_data.{self.table_name};")
        run_sql(
            f"""CREATE TABLE tiger_data.{self.table_name}(
                CONSTRAINT pk_{self.table_name} PRIMARY KEY (bg_id)
            ) INHERITS(tiger.bg);
            """
        )

    def finalized_table(self, connection: Connection):
        run_sql = lambda statement: connection.execute(text(statement))
        with connection.begin():
            run_sql(
                f"ALTER TABLE tiger_staging.{self.table_name} RENAME geoid TO bg_id"
            )
        self.fixup_staging_schema(connection)
        with connection.begin():
            run_sql(
                f"SELECT loader_load_staged_data(lower('{self.table_name}'), lower('{self.table_name}'));"
            )
            run_sql(
                f"ALTER TABLE tiger_data.{self.table_name} ADD CONSTRAINT chk_statefp CHECK (statefp = '{self.state_code}');"
            )
            run_sql(
                f"CREATE INDEX tiger_data_{self.table_name}_the_geom_gist ON tiger_data.{self.table_name} USING gist(the_geom);"
            )


class LoadFaces(LoadCountyFeature, luigi.Task):
    feature_name = "faces"

    def create_table(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql(f"DROP TABLE IF EXISTS tiger_data.{self.table_name};")
        run_sql(
            f"""CREATE TABLE tiger_data.{self.table_name}(
                CONSTRAINT pk_{self.table_name} PRIMARY KEY (gid)
            ) INHERITS(tiger.faces);"""
        )

    def finalized_table(self, connection: Connection):
        run_sql = lambda statement: connection.execute(text(statement))
        self.fixup_staging_schema(connection)
        with connection.begin():
            run_sql(
                f"SELECT loader_load_staged_data(lower('{self.table_name}'), lower('{self.table_name}'));"
            )
            run_sql(
                f"CREATE INDEX tiger_data_{self.table_name}_the_geom_gist ON tiger_data.{self.table_name} USING gist(the_geom);"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.table_name}_tfid ON tiger_data.{self.table_name} USING btree (tfid);"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.table_name}_countyfp ON tiger_data.{self.table_name} USING btree (countyfp);"
            )


class LoadFeatureNames(LoadCountyFeature, luigi.Task):
    feature_name = "featnames"

    def create_table(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql(f"DROP TABLE IF EXISTS tiger_data.{self.table_name};")
        run_sql(
            f"""CREATE TABLE tiger_data.{self.table_name}(
                CONSTRAINT pk_{self.table_name} PRIMARY KEY (gid))
            INHERITS(tiger.featnames);
            """
        )
        run_sql(
            f"ALTER TABLE tiger_data.{self.state_usps}_featnames ALTER COLUMN statefp SET DEFAULT '{self.state_code}'"
        )

    def finalized_table(self, connection: Connection):
        run_sql = lambda statement: connection.execute(text(statement))
        self.fixup_staging_schema(connection)
        with connection.begin():
            run_sql(
                f"SELECT loader_load_staged_data(lower('{self.table_name}'), lower('{self.table_name}'));"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.table_name}_snd_name ON tiger_data.{self.table_name} USING btree (soundex(name));"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.table_name}_lname ON tiger_data.{self.table_name} USING btree (lower(name));"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.table_name}_tlid_statefp ON tiger_data.{self.table_name} USING btree (tlid, statefp);"
            )


class LoadEdges(LoadCountyFeature, luigi.Task):
    feature_name = "edges"

    def requires(self):
        yield LoadStatePlaceFeature(year=self.year, state_usps=self.state_usps)
        yield LoadFaces(year=self.year, state_usps=self.state_usps)

    def create_table(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql(f"DROP TABLE IF EXISTS tiger_data.{self.table_name};")
        run_sql(
            f"""CREATE TABLE tiger_data.{self.table_name} (
                CONSTRAINT pk_{self.table_name} PRIMARY KEY (gid)
            ) INHERITS(tiger.edges);"""
        )

    def finalized_table(self, connection: Connection):
        run_sql = lambda statement: connection.execute(text(statement))
        self.fixup_staging_schema(connection)
        with connection.begin():
            run_sql(
                f"SELECT loader_load_staged_data(lower('{self.table_name}'), lower('{self.table_name}'));"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.table_name}_tlid ON tiger_data.{self.table_name} USING btree (tlid);"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.table_name}_tfidr ON tiger_data.{self.table_name} USING btree (tfidr);"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.table_name}_tfidl ON tiger_data.{self.table_name} USING btree (tfidl);"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.table_name}_countyfp ON tiger_data.{self.table_name} USING btree (countyfp);"
            )
            run_sql(
                f"CREATE INDEX tiger_data_{self.table_name}_the_geom_gist ON tiger_data.{self.table_name} USING gist(the_geom);"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.table_name}_zipl ON tiger_data.{self.table_name} USING btree (zipl);"
            )
            run_sql(
                f"DROP TABLE IF EXISTS tiger_data.{self.state_usps.upper()}_state_loc;"
            )
            run_sql(
                f"""CREATE TABLE tiger_data.{self.state_usps.upper()}_state_loc(
                    CONSTRAINT pk_{self.state_usps.upper()}_state_loc PRIMARY KEY(zip, stusps, place)
                ) INHERITS(tiger.zip_state_loc);"""
            )
            run_sql(
                f"""INSERT INTO tiger_data.{self.state_usps.upper()}_state_loc(zip, stusps, statefp, place)
                    SELECT DISTINCT e.zipl, '{self.state_usps.upper()}', '{self.state_code}', p.name
                        FROM tiger_data.{self.table_name} AS e
                        INNER JOIN tiger_data.{self.state_usps}_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid)
                        INNER JOIN tiger_data.{self.state_usps}_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp )
                    WHERE e.zipl IS NOT NULL;"""
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.state_usps.upper()}_state_loc_place ON tiger_data.{self.state_usps.upper()}_state_loc USING btree(soundex(place));"
            )
            run_sql(
                f"ALTER TABLE tiger_data.{self.state_usps.upper()}_state_loc ADD CONSTRAINT chk_statefp CHECK (statefp = '{self.state_code}');"
            )
            run_sql(
                f"DROP TABLE IF EXISTS tiger_data.{self.state_usps.upper()}_lookup_base;"
            )
            run_sql(
                f"""CREATE TABLE tiger_data.{self.state_usps.upper()}_lookup_base(
                    CONSTRAINT pk_{self.state_usps.upper()}_state_loc_city
                    PRIMARY KEY(zip,state, county, city, statefp)
                    ) INHERITS(tiger.zip_lookup_base);"""
            )
            run_sql(
                f"""INSERT INTO tiger_data.{self.state_usps.upper()}_lookup_base(zip,state,county,city, statefp)
                    SELECT DISTINCT e.zipl, '{self.state_usps.upper()}', c.name,p.name,'{self.state_code}'
                        FROM tiger_data.{self.table_name} AS e
                        INNER JOIN tiger.county As c  ON (e.countyfp = c.countyfp AND e.statefp = c.statefp AND e.statefp = '{self.state_code}')
                        INNER JOIN tiger_data.{self.state_usps}_faces AS f ON (e.tfidl = f.tfid OR e.tfidr = f.tfid)
                        INNER JOIN tiger_data.{self.state_usps}_place As p ON(f.statefp = p.statefp AND f.placefp = p.placefp )
                    WHERE e.zipl IS NOT NULL;"""
            )
            run_sql(
                f"ALTER TABLE tiger_data.{self.state_usps.upper()}_lookup_base ADD CONSTRAINT chk_statefp CHECK (statefp = '{self.state_code}');"
            )
            run_sql(
                f"CREATE INDEX idx_tiger_data_{self.state_usps.upper()}_lookup_base_citysnd ON tiger_data.{self.state_usps.upper()}_lookup_base USING btree(soundex(city));"
            )

    def vacuum(self, engine):
        run_raw_sql(engine, f"vacuum analyze tiger_data.{self.table_name};")
        run_raw_sql(
            engine, f"vacuum analyze tiger_data.{self.state_usps.upper()}_state_loc;"
        )


class LoadAddr(LoadCountyFeature, luigi.Task):
    feature_name = "addr"

    def create_table(self, connection: Connection) -> None:
        run_sql = lambda statement: connection.execute(text(statement))
        run_sql(f"DROP TABLE IF EXISTS tiger_data.{self.table_name};")
        run_sql(
            f"""
            CREATE TABLE tiger_data.{self.table_name}(
                CONSTRAINT pk_{self.table_name} PRIMARY KEY (gid))
            INHERITS(tiger.addr);
        """
        )
        run_sql(
            f"ALTER TABLE tiger_data.{self.table_name} ALTER COLUMN statefp SET DEFAULT '{self.state_code}';"
        )

    def finalized_table(self, connection: Connection):
        run_sql = lambda statement: connection.execute(text(statement))
        self.fixup_staging_schema(connection)
        with connection.begin():
            run_sql(
                f"SELECT loader_load_staged_data(lower('{self.table_name}'), lower('{self.table_name}'));"
            )


class LoadStateFeatures(luigi.WrapperTask):
    year = luigi.IntParameter(default=2019)
    state_usps = luigi.Parameter()
    resources = {"max_workers": 1}
    load_tab_blocks = luigi.BoolParameter(default=False)
    load_block_groups = luigi.BoolParameter(default=False)
    load_place_features = luigi.BoolParameter(default=False)
    load_faces = luigi.BoolParameter(default=False)
    load_all = luigi.BoolParameter(default=False)

    def requires(self):
        yield LoadStateCountySubdivisions(year=self.year, state_usps=self.state_usps)
        yield LoadStateTracts(year=self.year, state_usps=self.state_usps)
        yield LoadFeatureNames(year=self.year, state_usps=self.state_usps)
        yield LoadEdges(year=self.year, state_usps=self.state_usps)
        yield LoadAddr(year=self.year, state_usps=self.state_usps)

        if self.load_place_features or self.load_all:
            yield LoadStatePlaceFeature(year=self.year, state_usps=self.state_usps)
        if self.load_faces or self.load_all:
            yield LoadFaces(year=self.year, state_usps=self.state_usps)
        if self.load_tab_blocks or self.load_all:
            yield LoadTabBlocks10(year=self.year, state_usps=self.state_usps)
            if self.year > 2019:
                yield LoadTabBlocks20(year=self.year, state_usps=self.state_usps)
        if self.load_block_groups or self.load_all:
            yield LoadBlockGroups(year=self.year, state_usps=self.state_usps)


class LoadAllStateFeatures(luigi.Task):
    task_complete = False
    year = luigi.IntParameter(default=2019)
    load_tab_blocks = luigi.BoolParameter(default=False)
    load_block_groups = luigi.BoolParameter(default=False)
    load_place_features = luigi.BoolParameter(default=False)
    load_faces = luigi.BoolParameter(default=False)
    load_all = luigi.BoolParameter(default=False)

    STATE_USPSES = [
        "WV",
        "FL",
        "IL",
        "MN",
        "MD",
        "RI",
        "ID",
        "NH",
        "NC",
        "VT",
        "CT",
        "DE",
        "NM",
        "CA",
        "NJ",
        "WI",
        "OR",
        "NE",
        "PA",
        "WA",
        "LA",
        "GA",
        "AL",
        "UT",
        "OH",
        "TX",
        "CO",
        "SC",
        "OK",
        "TN",
        "WY",
        "HI",
        "ND",
        "KY",
        "ME",
        "NY",
        "NV",
        "AK",
        "MI",
        "AR",
        "MS",
        "MO",
        "MT",
        "KS",
        "IN",
        "SD",
        "MA",
        "VA",
        "DC",
        "IA",
        "AZ",
    ]

    def requires(self):
        yield LoadNationalData(year=self.year)
        for state_usps in self.STATE_USPSES:
            yield LoadStateFeatures(
                year=self.year,
                state_usps=state_usps,
                load_tab_blocks=self.load_tab_blocks,
                load_block_groups=self.load_block_groups,
                load_place_features=self.load_place_features,
                load_faces=self.load_faces,
                load_all=self.load_all,
            )

    def run(self):
        engine = create_engine(DB_CONN)
        create_indexes_and_vacuum(engine)
        self.task_complete = True

    def complete(self):
        return self.task_complete
