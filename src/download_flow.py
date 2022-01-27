#!/usr/bin/env python3
"""Run a sentinel download workflow."""
import json
import os
import sys
from pathlib import Path

from metaflow import FlowSpec, step, Parameter
import geopandas as gpd

try:
    import product_finder
    import senprep
    from cache_db import CacheDB
except:
    from src import product_finder
    from src import senprep
    from src.cache_db import CacheDB


SENTINEL_ROOT = "/var/satellite-data/"


class SentinelDownload(FlowSpec):
    """SenPrep v2. Implemented through callbacks."""

    config = Parameter("config", help="Configuration json", required=True)
    credentials_file = Parameter(
        "credentials", help="SentinelSat Credentials", required=True
    )
    credentials_file_earthdata = Parameter(
        "credentials_earthdata", help="Earthdata Credentials", required=True
    )
    credentials_file_google = Parameter(
        "credentials_google", help="Google Cloud Storage Credentials", required=True
    )

    mount = Parameter(
        "mount", help="Where the current dir is mounted", required=False)

    db_config = Parameter("db_config", help="JSON file with DB configuration",
                          required=False)
    outdir = Parameter("outdir", help="Where to save data", required=False)

    @step
    def start(self):
        """Read and validate configuration.

        This step ensures that the configuration has the following fields:
        - dates
        - callback_find_products
        - geojson
        """
        # Need to wrap the metaflow parameters to modify with the mount path
        # since we're potentially/usually running from within Docker
        curdir = os.getcwd()
        if self.mount:
            curdir = self.mount
        self.cfg = json.load(open(os.path.join(curdir, self.config), "r"))
        self.credentials = os.path.join(curdir, self.credentials_file)
        self.credentials_ed = os.path.join(
            curdir, self.credentials_file_earthdata)
        self.credentials_gcs = os.path.join(
            curdir, self.credentials_file_google)

        self.dir_out = SENTINEL_ROOT
        if self.outdir:
            self.dir_out = self.outdir
            SENTINEL_ROOT = self.outdir
            senprep.SENTINEL_ROOT = self.outdir
        print("Saving to", self.dir_out)

        assert (
            "dates" in self.cfg
        ), "Need to include (yyyymmdd, yyyymmdd) start and end dates."
        assert (
            "callback_find_products" in self.cfg
        ), "Need a SentinelProductFinder callback."
        assert "geojson" in self.cfg, "Need a geojson region of interest."
        self.do_cache = False
        self.cache_db_config = dict()
        if self.db_config:
            self.cache_db_config = json.load(
                open(os.path.join(curdir, self.db_config)))
            db = CacheDB(self.cfg, self.cache_db_config)
            print("Cache DB instance set up for flow.")
            db.add_config()
            db.add_roi()
        print("FINDER", self.cfg["callback_find_products"])
        self.next(self.find_products)

    @step
    def find_products(self):
        """Find products using a callback from `product_finder.SentinelProductFinder`.

        The callback is ran to generate a list of ids by querying and filtering the sentinelsat api.

        For download, all we need is for the product_finder to return a dict something like
        {'ids': [(id1, id2), (id3, id4)]}, which will be flattened to download each of
        [id1, id2, id3, id4]
        """
        finder = getattr(product_finder, self.cfg["callback_find_products"])
        if self.cache_db_config:  # if we have a config, we should cache
            db = CacheDB(self.cfg, self.cache_db_config)

        self.product_list, self.other_find_results = finder(
            self.cfg, self.credentials)

        self.products = []
        for (product_set_num, product_set) in enumerate(self.product_list):
            product = product_set["ids"]
            self.products.extend(product)
            if self.cache_db_config:  # if we have a config, we should cache
                df = gpd.GeoDataFrame(
                    product, geometry="geometry", crs="epsg:4326"
                ).reset_index(drop=True)
                db.add_sentinelsat_mirror(df)
                db.add_config_response(self.cfg, product_set_num, df)
        self.next(self.download)

    @step
    def download(self):
        """ForEach found product, download."""
        self.failed = []
        self.downloaded = []
        api = senprep.load_api(self.credentials)
        earthdata_auth = None
        if self.credentials_ed and Path(self.credentials_ed).exists():
            earthdata_auth = json.load(open(self.credentials_ed))
        already_downloaded_title = []
        self.already_downloaded_uuid = []
        if self.cache_db_config:  # if we have a config, we should cache
            db = CacheDB(self.cfg, self.cache_db_config)
            already_downloaded_title = db.get_results(
                "zip").result_location.apply(lambda x: Path(x).stem).tolist()
        for i, product in enumerate(self.products):
            if product.title in already_downloaded_title:
                self.already_downloaded_uuid.append(product.uuid)
                continue
            print(
                "DL {i}/{n}: {uuid}".format(
                    i=i + 1,
                    n=len(self.products),
                    uuid=product.uuid,
                ),
                end="",
            )
            metadata = api.get_product_odata(product.uuid)
            s1_or_s2 = metadata["title"][:2].lower()
            result = False
            # Sentinelsat version of download currently not used
            # as had some issues with timeouts and parallelisation downloads.
            # if metadata["Online"] == True:
            #     print(" - (online -> SentinelSat)")
            #     result = api.download(
            #         product.uuid, directory_path=self.dir_out, checksum=True)
            if s1_or_s2 == "s2":
                print(" - (offline S2 -> GCS)")
                result = senprep.download_S2_GCS_py(product, credentials=self.credentials_file_google, outdir=self.dir_out)
            elif s1_or_s2 == "s1":
                print(" - (offline S1 -> NOAA)")
                if not earthdata_auth:
                    print("NO EARTHDATA CREDENTIALS. FAIL.")
                else:
                    result = senprep.download_S1_NOAA_py(
                        product, 
                        auth=earthdata_auth, 
                        outdir=self.dir_out
                    )
            else:
                raise ValueError("Invalid odata. No alternate downloader for offline product.")
            if result != 0:
                self.failed.append((product.uuid, result))
            else:
                self.downloaded.append(product.uuid)
                if self.cache_db_config:  # if we have a config, we should cache
                    download_filename = f"<SENTINEL_ROOT>/{metadata['title']}.zip"
                    db.add_config_result("zip", download_filename)
                    print(
                        f"Added zip {download_filename} to cache 'config_results'")
        self.next(self.end)

    @step
    def end(self):
        """Display to user what has been downloaded."""
        for product in self.downloaded:
            print("DOWNLOADED {}".format(product))
        for product in self.already_downloaded_uuid:
            print("ALREADY DOWNLOADED {}".format(product))
        for product, reason in self.failed:
            print("FAILED {}: {}".format(product, reason))
        return


if __name__ == "__main__":
    SentinelDownload()
