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
from metaflow import FlowSpec, step, Parameter
from osgeo import gdal

# local
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
    njobs = Parameter(
        "njobs", help="How many jobs to run in parallel", required=False, default=1
    )
    rebuild = Parameter(
        "rebuild", help="Force gpt to re-collocate", required=False, default=False
    )
    db_config = Parameter("db_config", help="JSON file with DB configuration",
                          required=False)
    outdir = Parameter("outdir", help="Where to save data", required=False)

    @step
    def start(self):
        """Read and validate configuration."""
        global SENTINEL_ROOT
        curdir = os.getcwd()
        if self.mount:
            curdir = self.mount
        self.cfg = json.load(open(os.path.join(curdir, self.config), "r"))
        self.credentials = os.path.join(curdir, self.credentials_file)
        if self.outdir:
            SENTINEL_ROOT = self.outdir
            senprep.SENTINEL_ROOT = self.outdir
        assert (
            "dates" in self.cfg
        ), "Need to include (yyyymmdd, yyyymmdd) start and end dates."
        assert (
            "callback_find_products" in self.cfg
        ), "Need a SentinelProductFinder callback."
        assert "callback_snap" in self.cfg, "Need a Snapper callback."
        assert "geojson" in self.cfg, "Need a geojson region of interest."
        print("FINDER", self.cfg["callback_find_products"])
        print("SNAPPER", self.cfg["callback_snap"])
        if self.db_config:
            self.cache_db_config = json.load(
                open(os.path.join(curdir, self.db_config)))
            db = CacheDB(self.cfg, self.cache_db_config)
            print("Cache DB instance set up for flow.")
            db.add_config(self.cfg)
            db.add_roi(self.cfg)
        self.next(self.find_products)

    @step
    def find_products(self):
        """Find products using a callback from `product_finder.SentinelProductFinder`."""
        finder = getattr(product_finder, self.cfg["callback_find_products"])
        self.product_list, self.other_find_results = finder(
            self.cfg, self.credentials)
        print(len(self.product_list), "sets of products found")

        if self.cache_db_config:  # if we have a config, we should cache
            db = CacheDB(self.cache_db_config)

        for (product_set_num, product_set) in enumerate(self.product_list):
            product = product_set["ids"]
            df = gpd.GeoDataFrame(
                product, geometry="geometry", crs="epsg:4326"
            ).reset_index(drop=True)
            if self.cache_db_config:  # if we have a config, we should cache
                df = gpd.GeoDataFrame(
                    product, geometry="geometry", crs="epsg:4326"
                ).reset_index(drop=True)
                db.add_sentinelsat_mirror(df)
                db.add_config_response(product_set_num, df)

        n_per_job = math.ceil(len(self.product_list) / self.njobs)
        self.job_data = [
            self.product_list[i * n_per_job: (i + 1) * n_per_job]
            for i in range(self.njobs)
        ]
        print("Collocation can take HOURS if the area is large.")
        self.next(self.run_snap, foreach="job_data")

    @step
    def run_snap(self):
        """For each found product set, run the snap graph."""
        self.to_clip = []
        self.failed = []
        self.job = self.input
        if self.cache_db_config:  # if we have a config, we should cache
            db = CacheDB(self.cache_db_config)
            existing_collocations = db.get_results(
                config).query('result_type == "collocation"')

        for i, sublist in enumerate(self.job):
            # sublist is [{'ids': <sentinelsat_rows>, 'info': {'roi': <shape>, 'roi_no': int}}...]
            # it's length is a batch of TOTAL_PRODUCTS / NJOBS

            # TODO ... query database for existing collocations
            # ... and skip if necessary
            # if EXISTING:
            #     continue
            print(
                "Running snap for sub-list {i} of {n}".format(
                    i=i,
                    n=len(self.input),
                )
            )
            sys.stdout.flush()
            snap_func = getattr(snapper, self.cfg["callback_snap"])
            try:
                collocations = snap_func(
                    sublist, self.cfg, self.mount, self.rebuild)
                for _s1_or_s2, fn_collocation in collocations:
                    fn = str(fn_collocation).replace(
                        SENTINEL_ROOT, "<SENTINEL_ROOT>/")
                    print(f"ADDING COLLOCATION {fn}")
                    db.add_config_result("collocation", fn)
                self.to_clip.extend(collocations)
            except Exception as E:
                self.failed.append((E, sublist))
        self.next(self.crop)

    @step
    def crop(self):
        self.to_patch = []
        if self.cache_db_config:  # if we have a config, we should cache
            db = CacheDB(self.cache_db_config)
        for s1_or_s2, fn_collocate in self.to_clip:
            # fn_collocate: <SENTINEL_ROOT>/<name>/ROI/S1/Collocated/S1_abc_S2_def.tif
            # stem => S1_abc_S2_def ==> s1_uuid = abc, s2_uuid = def
            _, s1uuid, _, s2uuid = fn_collocate.stem.split(
                "_"
            )
            cropdir = (
                fn_collocate.parent.parent / "Clipped"
            )  # go back to the /S1/ folder, as per above example
            roi_dir = fn_collocate.parent.parent.parent  # e.g. the ../ROI1/ part
            roi_no = roi_dir.name.replace("ROI", "")  # e.g. the '1' from ROI1
            roi_path = roi_dir / f"ROI{roi_no}.geojson"

            cropdir.mkdir(exist_ok=True)

            if s1_or_s2 == "S1":
                crop_filename = f"S1_roi{roi_no}_{s1uuid}.tif"
            else:
                crop_filename = f"S2_roi{roi_no}_{s2uuid}.tif"
            path_crop = cropdir / crop_filename

            gdal_result = gdal.Warp(
                str(path_crop),
                str(fn_collocate),
                cutlineDSName=str(roi_path),
                cropToCutline=True,
                dstNodata=999999999.0,
            )
            gdal_result = None
            # TODO -- Do we need gdal.Warp twice for each object?
            gdal.Warp(
                str(path_crop),
                str(fn_collocate),
                cutlineDSName=str(roi_path),
                cropToCutline=True,
                dstNodata=999999999.0,
            )
            path_crop_tidy = str(path_crop).replace(
                SENTINEL_ROOT, "<SENTINEL_ROOT>/")
            db.add_config_result("crop", path_crop_tidy)
            self.to_patch.append((s1_or_s2, path_crop, fn_collocate))
        self.next(self.make_patches)

    @step
    def make_patches(self):
        self.n_patches = 0
        height, width = self.cfg["size"]
        self.patches = []
        for s1_or_s2, fn_cropped, fn_collocate in self.to_patch:
            # fn_cropped is like <SENTINEL_ROOT>/<name>/ROI<roi_no>/<S1_or_S2>/Clipped/<S1_or_S2>_roi<num>_uuid.tif
            patchdir = fn_cropped.parent / "PATCHES"
            patchdir.mkdir(exist_ok=True)

            _, s1uuid, _, s2uuid = fn_collocate.stem.split(
                "_"
            )  # e.g. s1_uuid = abc, s2_uuid = def

            raster = rio.open(str(fn_cropped))
            raster_im = raster.read(masked=False)
            res = int(next(raster).res[0])
            gdal_dataset = gdal.Open(str(fn_cropped))

            row_starts = np.arange(0, raster_im.shape[1], height)
            row_ends = row_starts + height - 1
            col_starts = np.arange(0, raster_im.shape[2], width)
            col_ends = col_starts + width - 1

            for row_start, row_end in zip(row_starts, row_ends):
                for col_start, col_end in zip(col_starts, col_ends):
                    patch_filename = f"S1_{s1uuid}_S2_{s2uuid}_{row_start}_{col_start}_{width}x{height}.tif"
                    path_patch = str(patchdir / patch_filename)
                    self.patches.append(path_patch)

                    start_x, start_y = raster.xy(row_start, col_start)
                    start_x = start_x - res / 2
                    start_y = start_y + res / 2

                    end_x, end_y = raster.xy(row_end, col_end)
                    end_x = start_x + res / 2
                    end_y = start_y - res / 2

                    projWin = [start_x, start_y, end_x, end_y]
                    gdal.Translate(
                        path_patch, gdal_dataset, format="GTiff", projWin=projWin
                    )
                    self.n_patches += 1
            raster.close()
            # TODO finish
        # TODO uncomment if we decide to export a zip of patches back to end users
        # with zipfile.ZipFile("/tmp/patches.zip", "w") as zf:
        #     for patch_filename in self.patches:
        #         zf.write(patch_filename)
        print(self.n_patches, "patches made")
        self.next(self.join)

    @step
    def join(self, inputs):
        self.failures = []
        self.to_clip = []
        # inputs are whatever was set to 'self' during the previous stages, since the 'foreach',
        # run_snap(), crop(), make_patches()
        for inp in inputs:
            self.failures.extend(inp.failed)
        self.next(self.end)

    @step
    def end(self):
        """Display to user what has been downloaded."""
        for msg, sublist in self.failures:
            ids = " ".join([prod.uuid for prod in sublist["ids"]])
            print("Snap FAILED {} for {}".format(msg, ids))
        return


if __name__ == "__main__":
    SnapProcess()
