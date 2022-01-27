#!/usr/bin/env python3
"""Run a sentinel download workflow."""
# std. lib
import json
import os
import sys
import math
import itertools
from argparse import ArgumentParser
from pathlib import Path

# external
import numpy as np
import rasterio as rio
from osgeo import gdal
from omegaconf import OmegaConf

# local
import product_finder
import snapper
import senprep
import roiutil


SENTINEL_ROOT = "/var/satellite-data/"


def find_products(config, credentials, njobs=1):
    """Find products using a callback from `product_finder.SentinelProductFinder`."""
    finder = getattr(product_finder, config["callback_find_products"])
    product_list, other_find_results = finder(config, credentials)
    return product_list, other_find_results


def crop_image(s1_or_s2, filename):
    # filename: <SENTINEL_ROOT>/<name>/ROI/S1/Collocated/S1_abc_S2_def.tif
    if not isinstance(filename, Path):
        filename = Path(filename)
    _, s1uuid, _, s2uuid = filename.stem.split("_")  # e.g. s1_uuid = abc, s2_uuid = def
    cropdir = (
        filename.parent.parent / "Clipped"
    )  # go back to the /S1/ folder, as per above example
    roi_dir = filename.parent.parent.parent  # e.g. the ../ROI1/ part
    roi_no = roi_dir.name.replace("ROI", "")  # e.g. the '1' from ROI1
    roi_path = roi_dir / f"ROI{roi_no}.geojson"

    cropdir.mkdir(exist_ok=True)

    if s1_or_s2 == "S1":
        crop_filename = f"S1_roi{roi_no}_{s1uuid}.tif"
    else:
        crop_filename = f"S2_roi{roi_no}_{s2uuid}.tif"
    path_crop = cropdir / crop_filename
    if path_crop.exists():
        print("CACHED CROP:", path_crop)
        return (s1_or_s2, path_crop, filename)

    gdal_result = gdal.Warp(
        str(path_crop),
        str(filename),
        cutlineDSName=str(roi_path),
        cropToCutline=True,
        dstNodata=999999999.0,
    )
    return (s1_or_s2, path_crop, filename)


def crop(to_crop):
    to_patch = []
    for s1_or_s2, fn_collocate in to_crop:
        to_patch.append(crop_image(s1_or_s2, fn_colloate))
    return to_patch


def make_patches_from_image(s1_or_s2, filename_cropped, filename_collocated, config):
    # filename_cropped is like 
    # <SENTINEL_ROOT>/<name>/ROI<roi_no>/<S1_or_S2>/Clipped/<S1_or_S2>_roi<num>_uuid.tif
    patchdir = filename_cropped.parent / "PATCHES"
    patchdir.mkdir(exist_ok=True)

    # e.g. s1_uuid = abc, s2_uuid = def
    _, s1uuid, _, s2uuid = filename_collocated.stem.split("_")

    raster = rio.open(str(filename_cropped))
    raster_im = raster.read(masked=False)
    res = int(raster.res[0])
    gdal_dataset = gdal.Open(str(filename_cropped))

    height, width = config["size"]
    row_starts = np.arange(0, raster_im.shape[1], height)
    col_starts = np.arange(0, raster_im.shape[2], width)

    # basically, ...for r in rows: for c in cols... , but done upfront, outside the loop
    top_left_corners = itertools.product(col_starts, row_starts) 
    patches = []
    for (col, row) in top_left_corners:
        patch_filename = f"S1_{s1uuid}_S2_{s2uuid}_{row}_{col}_{width}x{height}.tif"
        path_patch = str(patchdir / patch_filename)
        patches.append(path_patch)
        gdal.Translate(path_patch, gdal_dataset, format="GTiff", srcWin=[col, row, width, height])
    raster.close()
    return patches


def make_patches(to_patch):
    height, width = config["size"]
    all_patches = []
    for s1_or_s2, fn_cropped, fn_collocate in to_patch:
        patches = make_patches_from_image(s1_or_s2, fn_cropped, fn_collocate)
        all_patches.extend(patches)
    return all_patches


def snap_flow_mapper(product_set, snap_function, config, mount=None, rebuild=False):
    """Run a single set of products through the workflow.

    Inputs
    ------
    product_set : list of N-tuple
        Something like...
            [(s1), (s1_2)...]
            [(s1, s2), (s1_2, s2_2)...]
            [(s1_old, s1, s2), (s1_old_2, s1_2, s2_2)...]
    snap_function : function
        a method from the snapper module.
        should expect the same N-tuple format as provided by
        callback_find_products in the config
    mount : Path-like
        Prefix to append to paths
        Used if we mount `pwd` somewhere with docker and want
        to use simpler paths
    rebuild : bool
        Whether to force the SNAP graph processing tool to run
        already-completed products

    Outputs
    -------
    failures : list
        product ids and failure message, if an error occurred
    n_patches : int
        total number of patches generated
    """

    # SNAP is the most important portion to parallelise
    # the (very approximate) process (approximate big-O notation) is
    # run_snap O(1)
    # crop O(n) -- snap will return N outputs, so crop will have to run N times
    # patch O(n^2) -- patch generation loop for each of N crop outputs
    filenames_collocated = snap_function(product_set, config, args.mount, args.rebuild)

    # filenames_collocated is something like:
    # [("S1", filename_s1_collocated), ("S2", filename_s2_collocated)]

    n_patches = 0
    all_patches = []
    for sat_type, filename in filenames_collocated:
        s1_or_s2, filename_cropped, filename_collocated = crop_image(sat_type, filename)
        patches = make_patches_from_image(
            s1_or_s2, filename_cropped, filename_collocated, config
        )
        all_patches.extend(patches)
    return len(all_patches)


if __name__ == "__main__":
    parser = ArgumentParser("snap_flow")
    parser.add_argument("--config", required=True)
    parser.add_argument("--credentials", required=True)
    parser.add_argument("--mount", required=False)
    parser.add_argument("--njobs", default=1, required=False)
    parser.add_argument("--rebuild", required=False, default=False)
    parser.add_argument("--output", help="Change output directory", required=False)
    args = parser.parse_args()

    config = args.config
    credentials = args.credentials

    # 'mount' is a helper arg when using docker, to specify where $PWD is mounted
    # to inside the docker image (e.g. -v $(pwd):/here/ suggests --mount "/here/")
    #
    # this lets us do '--config configurations/sample.json'
    # rather than     '--config /here/configurations/sample.json'
    if args.mount:
        config = os.path.join(args.mount, config)
        credentials = os.path.join(args.mount, credentials)

    if args.output:
        SENTINEL_ROOT = args.output
        senprep.SENTINEL_ROOT = args.output
        snapper.SENTINEL_ROOT = args.output

    # ========== Load and validate config
    config = OmegaConf.load(open(config))
    assert "callback_find_products" in config, "Need a callback to find product IDs"
    assert "callback_snap" in config, "Need a callback for running SNAP"
    assert "geojson" in config, "Need a geojson region of interest"
    print("FINDER:", config.callback_find_products)
    print("SNAPPER:", config.callback_snap)

    # ========= Find products
    product_sets, other_return_data = find_products(config, credentials)

    # ========= Run snap flow for each product
    # callback_snap defines the name of a function inside the snapper module
    # something like:
    #       f(product_tuple, config, mount, rebuild)
    snap_func = getattr(snapper, config.callback_snap)
    results = []
    for p_set in product_sets:
        results.append(snap_flow_mapper(p_set, snap_func, config))
    print()
    print(sum(results), "patches created from", len(product_sets), "sets of products")
