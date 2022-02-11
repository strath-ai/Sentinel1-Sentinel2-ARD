#!/usr/bin/env python3
# pylint: disable=E1101
"""Run a sentinel download workflow."""
# std. lib
import json
import os
import sys
import math
import zipfile
import itertools
import pathlib
import datetime
from pathlib import Path

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

# Stop GDAL printing both warnings and errors to STDERR
# e.g. """TIFFReadDirectory:Sum of Photometric type-related color channels and ExtraSamples doesn't match SamplesPerPixel.
# Defining non-color channels as ExtraSamples."""
gdal.PushErrorHandler('CPLQuietErrorHandler')
gdal.UseExceptions()  # Raise exception if GDAL errors

SENTINEL_ROOT = "/var/satellite-data/"


class SnapProcess(FlowSpec):
    """SenPrep v2. Implemented through callbacks."""
    config = Parameter("config", help="Configuration json", required=True)
    credentials_file = Parameter("credentials", help="SentinelSat Credentials", required=True)
    credentials_file_earthdata = Parameter("credentials_earthdata", help="Earthdata Credentials", required=True)
    credentials_file_google = Parameter("credentials_google", help="Google Cloud Storage Credentials", required=True)
    mount = Parameter("mount", help="Where the current dir is mounted", required=False)
    njobs = Parameter("j", help="How many jobs to run in parallel", required=False, default=1)
    rebuild = Parameter("rebuild", help="Force gpt to re-collocate", required=False, default=False)
    only_download = Parameter("nosnap", help="Don't snap", required=False, default=False)
    db_config = Parameter("db_config", help="JSON file with DB configuration", required=False)
    outdir = Parameter("outdir", help="Where to save data", required=False)

    @step
    def start(self):
        """Read and validate configuration."""
        global SENTINEL_ROOT
        self.start_time = datetime.datetime.now()
        curdir = os.getcwd()
        if self.mount:
            curdir = self.mount
        with open(os.path.join(curdir, self.config), "r") as f:
            self.cfg = json.load(f)
        # Credentials for sentinelsat, NOAA, and GCS
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
            snapper.SENTINEL_ROOT = self.outdir
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
        self.cache_db_config = None
        if self.db_config:
            with open(os.path.join(curdir, self.db_config), 'r') as f:
                self.cache_db_config = json.load(f)
            db = CacheDB(self.cfg, self.cache_db_config)
            print("Cache DB instance set up for flow.")
            db.add_config()
            db.add_roi()
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
        filter_clouds = self.cfg.get("cloud_mask_filtering", False)
        self.product_list, self.other_find_results = finder(
            self.cfg, self.credentials, cloud_mask_filtering=filter_clouds)
        print(len(self.product_list), "sets of products found")

        if self.cache_db_config:  # if we have a config, we should cache
            db = CacheDB(self.cfg, self.cache_db_config)

        self.products = []
        for (product_set_num, product_set) in enumerate(self.product_list):
            product = product_set["ids"]
            self.products.extend(product)
            df = gpd.GeoDataFrame(
                product, geometry="geometry", crs="epsg:4326"
            ).reset_index(drop=True)
            if self.cache_db_config:  # if we have a config, we should cache
                db.add_sentinelsat_mirror(df)
                db.add_config_response(product_set_num, df)
        self.next(self.download)

    @step
    def download(self):
        """ForEach found product, download.

        This will use NOAA for S1 products, and Google Cloud Storage for S2 products.
        Sentinelsat _can_ be used, but 1) it has limitations on numbers of parallel
        downloads, and 2) archived produts (more than a year old), must be 'woken up'
        before access, which can take up to 24 hours.
        """
        self.failed = []
        self.downloaded = []
        api = senprep.load_api(self.credentials)
        earthdata_auth = None
        if self.credentials_ed and Path(self.credentials_ed).exists():
            with open(self.credentials_ed, 'r') as f:
                earthdata_auth = json.load(f)
        already_downloaded_titles = []
        self.already_downloaded_uuid = []
        if self.cache_db_config:  # if we have a config, we should cache
            db = CacheDB(self.cfg, self.cache_db_config)
            already_downloaded_titles = db.get_results(
                "zip").result_location.apply(lambda x: Path(x).stem).tolist()
        to_download = [p for p in self.products if p.title not in already_downloaded_titles]
        products_already_downloaded = [p for p in self.products if p.title in already_downloaded_titles]
        for product in products_already_downloaded:
            self.already_downloaded_uuid.append(product.uuid)
        print(f"{len(products_already_downloaded)} already downloaded")
        print(f"{len(to_download)} to download")
        for i, product in enumerate(to_download):
            print("DL {i+1}/{len(to_download)}: {product.uuid}", end="")
            metadata = api.get_product_odata(product.uuid)
            s1_or_s2 = metadata["title"][:2].lower()
            result = False
            if s1_or_s2 == "s2":
                print(" - (S2 from GCS)")
                senprep.authenticate_google_cloud(credentials_file=self.credentials_file_google)
                result = senprep.download_S2_GCS(product, credentials=self.credentials_file_google, outdir=self.dir_out)
            elif s1_or_s2 == "s1":
                print(" - (S1 from NOAA)")
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
                    print(f"Added zip {download_filename} to cache 'config_results'")

        self.next(self.split_products_to_snap)

    @step
    def split_products_to_snap(self):
        """For each of the products needing processed, run the snap pipeline.

        If we flag --only-download, processing will be skipped from here onward."""
        n_per_job = math.ceil(len(self.product_list) / self.njobs)

        self.job_data = [
            self.product_list[i * n_per_job: (i + 1) * n_per_job]
            for i in range(self.njobs)
        ]
        print("Collocation can take HOURS if the area is large.")
        self.next(self.run_snap, foreach="job_data")

    @step
    def run_snap(self):
        """For each found product subset, run the snap graph."""
        self.to_clip = []
        self.failed = []
        self.job = self.input
        if self.only_download:
            print("Skipping snap processing.")
            self.job = []
        db = None
        if self.cache_db_config:  # if we have a config, we should cache
            db = CacheDB(self.cfg, self.cache_db_config)
            existing_collocations = db.get_results(result_type="collocation")

        for i, sublist in enumerate(self.job):
            # sublist is [{'ids': <sentinelsat_rows>, 'info': {'roi': <shape>, 'roi_no': int}}...]
            # it's length is a batch of TOTAL_PRODUCTS / NJOBS

            print(f"Running snap for sub-list {i} of {len(self.input)}")
            sys.stdout.flush()
            snap_func = getattr(snapper, self.cfg["callback_snap"])
            existing_results = []
            if not db is None:
                existing_results = db.get_results(result_type="collocation")
            try:
                collocations = snap_func(
                    sublist, self.cfg, self.mount, self.rebuild, outdir=self.dir_out)
                for _s1_or_s2, fn_collocation in collocations:
                    fn = str(fn_collocation).replace(
                        SENTINEL_ROOT, "<SENTINEL_ROOT>/")
                    if isinstance(existing_results, pd.DataFrame):
                        if not fn in existing_results.result_location:
                            print(f"ADDING COLLOCATION {fn}")
                            db.add_config_result("collocation", fn)
                    self.to_clip.append((_s1_or_s2, fn_collocation))
            except Exception as E:
                self.failed.append((E, sublist))
        self.next(self.crop)

    @step
    def crop(self):
        """Crop each image down to the region of interest."""
        self.to_patch = []
        db = None
        existing_results = []
        if self.cache_db_config:  # if we have a config, we should cache
            db = CacheDB(self.cfg, self.cache_db_config)
            existing_results = db.get_results(result_type="crop")

        def already_cropped(fn):
            if db is None:
                return fn.exists()
            fn_tidy = str(fn).replace(SENTINEL_ROOT, "<SENTINEL_ROOT>/")

            return fn_tidy in existing_results.result_location

        for s1_or_s2, fn_collocate in self.to_clip:
            # fn_collocate: <SENTINEL_ROOT>/<name>/ROI/S1/Collocated/S1_abc_S2_def.tif
            # stem => S1_abc_S2_def ==> s1_uuid = abc, s2_uuid = def
            _, s1uuid, _, s2uuid = fn_collocate.stem.split("_")
            cropdir = fn_collocate.parent.parent / "Clipped"  # go back to the /S1/ folder, as per above example
            roi_dir = fn_collocate.parent.parent.parent  # e.g. the ../ROI1/ part
            roi_no = roi_dir.name.replace("ROI", "")  # e.g. the '1' from ROI1
            roi_path = roi_dir / f"ROI{roi_no}.geojson"

            cropdir.mkdir(exist_ok=True)

            if s1_or_s2 == "S1":
                crop_filename = f"S1_roi{roi_no}_{s1uuid}.tif"
            else:
                crop_filename = f"S2_roi{roi_no}_{s2uuid}.tif"
            path_crop = cropdir / crop_filename
            path_crop_tidy = str(path_crop).replace(SENTINEL_ROOT, "<SENTINEL_ROOT>/")

            # Append to the to_patch list,
            # and let to_patch handle 'already existing' if needed
            # i.e. don't pre-empt any caching
            self.to_patch.append((s1_or_s2, path_crop, fn_collocate))
            if (not already_cropped(path_crop)) or self.rebuild:
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
                if db is not None:
                    db.add_config_result("crop", path_crop_tidy)
            else:
                print("CACHED CROP", crop_filename)
        self.next(self.make_patches)

    @step
    def make_patches(self):
        """Generate patches of user-defined size and overlap."""
        self.n_patches = 0
        height, width = self.cfg["size"]
        self.patches = []
        # existing_results = db.get_results(result_type="patch")
        for s1_or_s2, fn_cropped, fn_collocate in self.to_patch:
            # fn_cropped is like
            # <SENTINEL_ROOT>/<name>/ROI<roi_no>/<S1_or_S2>/Clipped/<S1_or_S2>_roi<num>_uuid.tif
            # so patchdir will be
            # <SENTINEL_ROOT>/<name>/ROI<roi_no>/<S1_or_S2>/Clipped/PATCHES
            patchdir = fn_cropped.parent / "PATCHES"
            patchdir.mkdir(exist_ok=True)

            # fn_collocate is like `S1_<uuid>_S2_<uuid>.tif`
            _, s1uuid, _, s2uuid = fn_collocate.stem.split("_")

            raster = rio.open(str(fn_cropped))
            raster_im = raster.read(masked=False)
            res = int(raster.res[0])
            gdal_dataset = gdal.Open(str(fn_cropped))

            row_starts = np.arange(0, raster_im.shape[1], height)
            col_starts = np.arange(0, raster_im.shape[2], width)

            # basically, ...for r in rows: for c in cols... , but done outside a loop
            top_left_corners = itertools.product(col_starts, row_starts)
            for (col, row) in top_left_corners:
                patch_filename = f"S1_{s1uuid}_S2_{s2uuid}_{row}_{col}_{width}x{height}.tif"
                path_patch = str(patchdir / patch_filename)
                self.patches.append(path_patch)
                gdal.Translate(path_patch, gdal_dataset, format="GTiff", srcWin=[col, row, width, height])
            raster.close()
        self.n_patches = len(self.patches)

        self.next(self.join)

    @step
    def join(self, inputs):
        """Aggregate the results of each parallel processing pipeline."""
        self.failures = []
        self.to_clip = []
        # inputs are whatever was set to 'self' during the previous stages, since the 'foreach',
        # run_snap(), crop(), make_patches()
        # e.g. 'inp' is like an instance of SnapFlow
        # and inp.patches will be the whatever was set to self.patches during each
        # of the parallel workloads dispatched from the 'foreach'
        self.all_patches = []
        self.cfg = None
        self.dir_out = None
        self.start_time = None
        for inp in inputs:
            self.all_patches.extend(inp.patches)
            self.failures.extend(inp.failed)

            # Need to re-assign cfg as the metaflow sub-job does not have it after a 'foreach' split
            # and need it for the overall-zip phase
            if self.cfg is None:
                self.cfg = inp.cfg
            if self.dir_out is None:
                self.dir_out = inp.dir_out
            if self.start_time is None:
                self.start_time = inp.start_time

        self.next(self.zip_patches)

    @step
    def zip_patches(self):
        """Zip up all the patches created during earlier steps."""
        zipname = pathlib.Path(self.dir_out) / "Sentinel_Patches" / self.cfg["name"] / "patches.zip"
        if self.all_patches:
            with zipfile.ZipFile(str(zipname), "w") as zf:
                for patch_filename in self.all_patches:
                    zf.write(patch_filename)
            print(len(self.all_patches), "patches made, and zipped to", zipname)

        self.next(self.end)

    @step
    def end(self):
        """Display to user what has been downloaded and processed."""
        for msg, sublist in self.failures:
            ids = " ".join([prod.uuid for prod in sublist["ids"]])
            print("Snap FAILED {} for {}".format(msg, ids))
        tot_time = datetime.datetime.now() - self.start_time
        name = self.cfg['name']
        print(f"Job '{name}' took {tot_time}")
        return


if __name__ == "__main__":
    SnapProcess()
