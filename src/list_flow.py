#!/usr/bin/env python3
"""Run a sentinel download workflow."""
# std. lib
import json
import os
import sys
import math
import zipfile

# external
import numpy as np
import rasterio as rio
import geopandas as gpd
import pandas as pd
from metaflow import FlowSpec, step, Parameter
from osgeo import gdal

# local
try:
    from src import product_finder
    from src import snapper
    from src import senprep
    from src.cache_db import CacheDB
except Exception as E:
    import product_finder
    import snapper
    import senprep
    from cache_db import CacheDB

SENTINEL_ROOT = "/var/satellite-data/"


class SnapProcess(FlowSpec):
    """SenPrep v2. Implemented through callbacks."""

    config = Parameter("config", help="Configuration json", required=True)
    credentials_file = Parameter(
        "credentials", help="SentinelSat Credentials", required=True)
    mount = Parameter("mount", help="Where the current dir is mounted",
                      required=False)

    @step
    def start(self):
        """Read and validate configuration."""
        global SENTINEL_ROOT
        curdir = os.getcwd()
        if self.mount:
            curdir = self.mount
        self.cfg = json.load(open(os.path.join(curdir, self.config), "r"))
        self.credentials = os.path.join(curdir, self.credentials_file)
        assert (
            "dates" in self.cfg
        ), "Need to include (yyyymmdd, yyyymmdd) start and end dates."
        assert (
            "callback_find_products" in self.cfg
        ), "Need a SentinelProductFinder callback."
        assert "geojson" in self.cfg, "Need a geojson region of interest."
        print("FINDER", self.cfg["callback_find_products"])
        self.next(self.find_products)

    @step
    def find_products(self):
        """Find products using a callback from `product_finder.SentinelProductFinder`."""
        finder = getattr(product_finder, self.cfg["callback_find_products"])
        filter_clouds = self.cfg.get("cloud_mask_filtering", False)
        self.product_list, self.other_find_results = finder(
            self.cfg, self.credentials, cloud_mask_filtering=filter_clouds)
        print(len(self.product_list), "sets of products found")
        print("Each is a tuple of length:", len(self.product_list[0]['ids']))

        for (product_set_num, product_set) in enumerate(self.product_list):
            product = product_set["ids"]
            df = gpd.GeoDataFrame(
                product, geometry="geometry", crs="epsg:4326"
            ).reset_index(drop=True)
            for j, item in enumerate(df.title.tolist()):
                print("{}/{}) {}".format(product_set_num+1, j+1, item))
        self.next(self.end)
    
    @step
    def end(self):
        """End. Do nothing."""
        return


if __name__ == "__main__":
    SnapProcess()
