#!/usr/bin/env python3
import pandas as pd
import sqlalchemy as sa
import geoalchemy2  # used for side-effects to sqlalchemy
from sqlalchemy.exc import IntegrityError
from psycopg2.errors import UniqueViolation
import geopandas as gpd
from hashlib import md5
from pathlib import Path
import json

try:
    from src import roiutil
except:
    import roiutil


SENTINELSAT_RESPONSE_SCHEMA = [
    ("title", "TEXT"),
    ("link", "TEXT"),
    ("link_alternative", "TEXT"),
    ("link_icon", "TEXT"),
    ("summary", "TEXT"),
    ("ondemand", "BOOLEAN"),
    ("beginposition", "TIMESTAMP"),
    ("endposition", "TIMESTAMP"),
    ("ingestiondate", "TIMESTAMP"),
    ("missiondatatakeid", "REAL"),
    ("orbitnumber", "INTEGER"),
    ("lastorbitnumber", "REAL"),
    ("relativeorbitnumber", "INTEGER"),
    ("lastrelativeorbitnumber", "REAL"),
    ("slicenumber", "REAL"),
    ("sensoroperationalmode", "TEXT"),
    ("swathidentifier", "TEXT"),
    ("orbitdirection", "TEXT"),
    ("producttype", "TEXT"),
    ("timeliness", "TEXT"),
    ("platformname", "TEXT"),
    ("platformidentifier", "TEXT"),
    ("instrumentname", "TEXT"),
    ("instrumentshortname", "TEXT"),
    ("filename", "TEXT"),
    ("format", "TEXT"),
    ("productclass", "TEXT"),
    ("polarisationmode", "TEXT"),
    ("acquisitiontype", "TEXT"),
    ("status", "TEXT"),
    ("size", "TEXT"),
    ("identifier", "TEXT"),
    ("uuid", "UUID PRIMARY KEY"),
    ("geometry", "GEOMETRY(MULTIPOLYGON,4326)"),
    ("overlap_area", "REAL"),
    ("Percent_area_covered", "REAL"),
]

# dependant on SENTINELSAT_RESPONSE, REGION_OF_INTEREST, and CONFIGURATIONS
SENTINELSAT_CONFIG_RESPONSE_SCHEMA = [
    ("id", "SERIAL PRIMARY KEY"),  # 'serial' is basically INTEGER AUTO INCREMENT
    ("config", "INTEGER"),  # foreign key configurations.id
    ("product_set", "INTEGER"),  # if we have N weeks this is 0..N
    ("uuid", "UUID"),  # foreign key sentinelsat_response.uuid
    ("roi_id", "INTEGER"),  # foreign key region_of_interest.id
]

# independant table
SENTINESAL_CONFIGURATIONS_SCHEMA = [
    ("id", "SERIAL PRIMARY KEY"),
    ("config", "JSON"),  # raw configuration
    ("hash", "TEXT"),  # md5 hash of the raw config
]

# dependant on CONFIGURATIONS
SENTINELSAT_CONFIG_RESULTS_SCHEMA = [
    ("config", "INTEGER"),  # foreign key configurations.id
    ("result_type", "SENPREP_RESULT"),  # collocation or zip
    ("result_location", "TEXT"),  # path to result on disk
]

# // Could probably just replace this with TEXT inside config_results
# Enum senprep_result {
#   collocation
#   zip
# }

# independant table
SENTINELSAT_REGION_OF_INTEREST_SCHEMA = [
    ("id", "SERIAL PRIMARY KEY"),
    ("roi", "GEOMETRY(MULTIPOLYGON, 4326)"),
]


class CacheDB:
    con: sa.engine.Engine
    # Raw configuration JSON, and a hash
    table_configurations = "configurations"
    # The rows returned by a sentinelsat query, and used to generate products
    table_sentinelsat_response = "sentinelsat_response"
    # the pairings of data sent to snapper. Refers to groups of uuids from sentinelsat_response
    table_config_response = "config_response"
    # Region of interest from the configuration
    table_region_of_interest = "region_of_interest"
    # Type of result (collocation, zip), and location on disk
    table_config_results = "config_results"
    config: dict

    def __init__(self, run_config, db_config, echo=False):
        """Initialise a CacheDB instance.

        Arguments
        ---------
        run_config: dict
            A configuration as from config_util

        db_config: dict
            username: str
                The username for the postgres database
            password: str
                The database password
            dbname: str
                Name of the database
            host: str
                IP / connection point for the database [default: localhost]
            port: int
                Port for the database [default: 5432]

        echo: bool
            Whether to show what SQL commands are actually being ran.

        Keyword Arguments
        -----------------
        """
        self.config = run_config
        username = db_config["username"]
        password = db_config["password"]
        host = db_config.get("host", "localhost")
        port = db_config.get("port", 5432)
        db_name = db_config["dbname"]
        self.con = sa.create_engine(
            f"postgresql://{username}:{password}@{host}:{port}/{db_name}", echo=echo
        )

    def add_config(self):
        config_json_str = json.dumps(self.config)
        config_hash = md5(config_json_str.encode()).hexdigest()

        try:
            self.con.execute(
                f"""
                INSERT INTO configurations (config, hash)
                VALUES ( '{config_json_str}', '{config_hash}' )
                """
            )
        except IntegrityError as e:
            if isinstance(e.orig, UniqueViolation):
                print(f"ALREADY IN DB: Config ({config_hash})")
            else:
                raise e

    def add_roi(self):
        poly = roiutil.ROI(self.config["geojson"]).to_multipolygon()
        self.con.execute(
            f"""
            insert into region_of_interest (roi)
            values ( st_geomfromtext('{poly}', 4326) )
            ON CONFLICT ON CONSTRAINT no_dup_roi
            DO NOTHING;
            """
        )

    def add_config_response(self, product_set_num: int, product_set: gpd.GeoDataFrame):
        """Add what UUIDS a config returns to the database."""
        config_id = self.get_config_id()
        roi_id = self.get_roi_id()

        for uuid in product_set.uuid.to_list():
            try:
                self.con.execute(
                    f"""
                        INSERT INTO config_response (config, product_set, uuid, roi_id)
                        VALUES ( '{config_id}', '{product_set_num}',
                                '{uuid}', '{roi_id}' )
                    """
                )
            except IntegrityError as e:
                if isinstance(e.orig, UniqueViolation):
                    print(
                        f"ALREADY IN DB: Config response ({product_set_num}, {uuid}) "
                    )
                else:
                    raise e

    def add_config_result(self, result_type: str, filename: str):
        roi = roiutil.ROI(self.config["geojson"]).to_multipolygon()
        config_id = self.get_config_id()

        self.con.execute(
            f"""
            INSERT INTO config_results (config_id, result_type, result_location)
            VALUES ( '{config_id}', '{result_type}', '{filename}' )
        """
        )

    def add_sentinelsat_mirror(self, sentinelsat_rows: gpd.GeoDataFrame):
        existing = [
            str(row[0])
            for row in self.con.execute(
                f"SELECT uuid from sentinelsat_response"
            ).fetchall()
        ]
        df_already_in_db = sentinelsat_rows[
            sentinelsat_rows.uuid.isin(existing)
        ].uuid.tolist()
        cols = [col[0].lower() for col in SENTINELSAT_RESPONSE_SCHEMA]
        for uuid in df_already_in_db:
            print(f"ALREADY IN DB: Sentinelsat_response {uuid}")
        sentinelsat_rows[~sentinelsat_rows.uuid.isin(existing)][cols].to_postgis(
            "sentinelsat_response", self.con, if_exists="append"
        )

    def get_sentinelsat_mirror(self) -> gpd.GeoDataFrame:
        config_id = self.get_config_id()
        sql = f"""
        SELECT * FROM sentinelsat_response
        WHERE uuid in (
            SELECT uuid FROM config_response
            WHERE config = '{config_id}' ) """
        df = gpd.GeoDataFrame.from_postgis(sql, self.con, geom_col="geometry")
        return df

    def get_roi_id(self):
        roi = roiutil.ROI(self.config["geojson"]).to_multipolygon()
        return self.con.execute(
            f"""select id from region_of_interest
            where roi = st_geomfromtext('{roi}', 4326)
            limit 1"""
        ).fetchall()[0][0]

    def get_config_id(self):
        config_json = json.dumps(self.config)
        return self.con.execute(
            f"""select id from configurations
            where hash = '{md5(config_json.encode()).hexdigest()}'
            limit 1"""
        ).fetchall()[0][0]

    def get_config_response(self):
        cols = [colname for (colname, coltype) in SENTINELSAT_CONFIG_RESPONSE_SCHEMA]
        rows = self.con.execute(
            f"""
            SELECT config, product_set, uuid, roi_id FROM config_response
            WHERE config = '{self.get_config_id()}'
        """
        ).fetchall()
        return pd.DataFrame(rows, columns=cols)

    def get_results(self, result_type=None, translate_dir=None):
        where_clause = ""
        if result_type:
            where_clause = f" AND result_type = '{result_type}'"
        rows = self.con.execute(
            f"""
            SELECT * FROM config_results
            WHERE config_id = '{self.get_config_id()}' {where_clause}"""
        ).fetchall()
        cols = [colname for (colname, coltype) in SENTINELSAT_CONFIG_RESULTS_SCHEMA]
        df = pd.DataFrame(rows, columns=cols)
        if translate_dir:
            df["result_location"] = df.result_location.apply(
                lambda x: Path(x.replace("<SENTINEL_ROOT>", translate_dir))
            )
        return df
