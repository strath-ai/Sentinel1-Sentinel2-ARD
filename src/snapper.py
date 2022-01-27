# std. lib
import os
import sys
import json
import subprocess
from pathlib import Path

# external
from sqlalchemy import create_engine
import pandas as pd
import rasterio

# local
try:
    from src import senprep
    from src import roiutil
except ImportError:
    import senprep
    import roiutil


SENTINEL_ROOT = Path("/var/satellite-data/")
SENTINEL_SNAPPERS = set()

# ==================================================
# WARNING
#
# This file should ONLY have callbacks for running
# the esa snap graph processing tool.
# All other code should be somewhere else,
# like senprep.py
# ==================================================


def snapper(func):
    """Adds a function to the list of available snappers.

    Mainly intended for interactive use, so when debugging or running jupyter,
    a user can just check snapper.SENTINEL_SNAPPERS to find out which callbacks
    are available, as snapper.__dict__ lists all imports etc."""
    SENTINEL_SNAPPERS.add(func.__name__)
    return func


@snapper
def s1(products):
    pass


@snapper
def s2(products):
    pass


@snapper
def s2_with_previous_s1(product_tuple, config, mount=None, rebuild=False, **kwargs):
    """Collocate Sen1 and Sen2 products.

    This is not designed for interactive use (e.g. jupyter), but it probably
    could be used. It's supposed to be used by a metaflow workflow, taken as
    a callback from a config script (e.g. "callback_snap": "S2_with_previous_S1")

    Arguments
    ---------
    product_tuple : [(S1_row, S2_row), (ROI_subset, ROI_no)]
        A pair of rows from separate sentinelsat queries, along with the roi and roi number
    config : dict with...
        "bands_S1" ... List[str] - which S1 bands to use
        "bands_S2" ... List[str] - which S2 bands to use
    """
    outdir = Path(kwargs.get('outdir', SENTINEL_ROOT))
    # save s1 row to sqlite table
    # s1 = pd.read_sql('select * from sat_output where uuid like '...')[0]
    s1, s2 = product_tuple["ids"]
    ROI_no = int(product_tuple["info"]["roi_no"])
    ROI_subset = int(product_tuple["info"]["roi"])
    s1_date = s1.beginposition.strftime("%Y%m%d")
    s2_date = s2.beginposition.strftime("%Y%m%d")
    s1_zip = str(outdir / f"""{s1["title"]}.zip""")
    s2_zip = str(outdir / f"""{s2["title"]}.zip""")

    imagename = f"""S1_{s1["uuid"]}_S2_{s2["uuid"]}.tif"""

    dir_out_for_roi = (
        outdir
        / "Sentinel_Patches"
        / config["name"]
        / senprep.nearest_previous_monday(s2.beginposition).strftime("%Y%m%d")
        / f"""ROI{ROI_no}"""
    )

    filename_s1_collocated = dir_out_for_roi / "S1" / "Collocated" / imagename
    filename_s2_collocated = dir_out_for_roi / "S2" / "Collocated" / imagename

    filename_s1_collocated.parent.mkdir(exist_ok=True, parents=True)
    filename_s2_collocated.parent.mkdir(exist_ok=True, parents=True)

    cache_db = create_engine("sqlite:///{}".format(outdir / "cache.db"))
    if "collocations" not in cache_db.table_names():
        cache_db.execute("CREATE TABLE collocations (filename string)")

    existing = pd.read_sql("collocations", cache_db)
    existing_s1 = str(filename_s1_collocated) in existing["filename"].values
    existing_s2 = str(filename_s2_collocated) in existing["filename"].values
    # Don't check for 'filename.exists()', as the file is created when snap STARTS, not when snap finishes,
    # so if the snap fails, the file will still be there.
    # Instead, check if there is a row in the 'collocations' table, and then add these collocation files
    # to the table before returning
    if existing_s1 and existing_s2 and not rebuild:
        print(f"""CACHED COLLOCATION: {s1["uuid"]} and {s2["uuid"]}""")
        return [("S1", filename_s1_collocated), ("S2", filename_s2_collocated)]

    # gpt complains if LD_LIBRARY_PATH is not set
    # for some reason, this works on jupyter, but not from terminal
    if "LD_LIBRARY_PATH" not in os.environ:
        os.environ["LD_LIBRARY_PATH"] = "."
    else:
        parts = os.environ["LD_LIBRARY_PATH"].split(":")
        if "." not in parts:
            os.environ["LD_LIBRARY_PATH"] += ":."

    gpt_file = "gpt_files/gpt_cloud_masks_bands_specified_subset_without_reprojection.xml"
    # gpt_file = "gpt_files/gpt_cloud_masks_bands_specified_subset.xml"
    if mount:
        gpt_file = str(Path(mount) / gpt_file)
    command = [
            "gpt",
            gpt_file,
            "-PS1={}".format(s1_zip),
            "-PS2={}".format(s2_zip),
            "-PCollocate_master={}".format(s2["title"]),
            "-PS1_write_path={}".format(filename_s1_collocated),
            "-PS2_write_path={}".format(filename_s2_collocated),
            "-Pbands_S1={}".format(",".join(config["bands_S1"])),
            "-Pbands_S2={}".format(",".join(config["bands_S2"])),
            "-e",
        ]
    # print("RUNNING", " ".join(command))
    proc_output = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    err = proc_output.returncode

    if err:
        output = proc_output.stdout.decode()
        if "out of bounds" in output:
            err_msg = (
                f"""gpt out of bounds error: {s1["uuid"]} and {s2["uuid"]}: {err}"""
            )
            raise senprep.CoordinateOutOfBoundsError(err_msg)
        raise Exception("Collocating: gpt return code %s: %s" % (err, output))

    # Cache successful products
    # here, we save the full filenames of the collocated products to a dictionary
    # that exists in the output location (e.g. something like /var/satellite-data/cache.db
    to_cache = []
    if not existing_s1:
        to_cache.append(f"('{filename_s1_collocated}')")
    if not existing_s2:
        to_cache.append(f"('{filename_s2_collocated}')")

    # The 'EXCEPT .. SELECT' ensures we do not end up with duplicate entries
    if to_cache:
        cache_db.execute(
            """
            INSERT INTO collocations
            VALUES {to_cache}
            EXCEPT SELECT filename from collocations
            """.format(
                to_cache=", ".join(to_cache)
            )
        )

    crs = rasterio.open(filename_s1_collocated).crs
    roiutil.export_to_file(ROI_subset, dir_out_for_roi /
                           f"ROI{ROI_no}.geojson", crs)
    return [("S1", filename_s1_collocated), ("S2", filename_s2_collocated)]


@snapper
def s2_with_previous_s1__subset(product_tuple, config, mount=None, rebuild=False, **kwargs):
    """Collocate Sen1 and Sen2 products.

    This is not designed for interactive use (e.g. jupyter), but it probably
    could be used. It's supposed to be used by a metaflow workflow, taken as
    a callback from a config script (e.g. "callback_snap": "S2_with_previous_S1")

    Arguments
    ---------
    product_tuple : [(S1_row, S2_row), (ROI_subset, ROI_no)]
        A pair of rows from separate sentinelsat queries, along with the roi and roi number
    config : dict with...
        "bands_S1" ... List[str] - which S1 bands to use
        "bands_S2" ... List[str] - which S2 bands to use
    """
    outdir = Path(kwargs.get('outdir', SENTINEL_ROOT))
    # save s1 row to sqlite table
    # s1 = pd.read_sql('select * from sat_output where uuid like '...')[0]
    s1, s2 = product_tuple["ids"]
    ROI_subset = product_tuple["info"]["roi"]
    ROI_no = int(product_tuple["info"]["roi_no"])
    s1_date = s1.beginposition.strftime("%Y%m%d")
    s2_date = s2.beginposition.strftime("%Y%m%d")
    s1_zip = outdir / f"""{s1["title"]}.zip"""
    s1_bare = outdir / f"""{s1["title"]}"""
    s1_safe = outdir / f"""{s1["title"]}.SAFE"""
    s2_zip = outdir / f"""{s2["title"]}.zip"""
    s2_bare = outdir / f"""{s2["title"]}"""
    s2_safe = outdir / f"""{s2["title"]}.SAFE"""

    imagename = f"""S1_{s1["uuid"]}_S2_{s2["uuid"]}.tif"""

    dir_out_for_roi = (
        outdir
        / "Sentinel_Patches"
        / config["name"]
        / senprep.nearest_previous_monday(s2.beginposition).strftime("%Y%m%d")
        / f"""ROI{ROI_no}"""
    )

    filename_s1_collocated = dir_out_for_roi / "S1" / "Collocated" / imagename
    filename_s2_collocated = dir_out_for_roi / "S2" / "Collocated" / imagename

    filename_s1_collocated.parent.mkdir(exist_ok=True, parents=True)
    filename_s2_collocated.parent.mkdir(exist_ok=True, parents=True)

    cache_db = create_engine("sqlite:///{}".format(outdir / "cache.db"))
    if "collocations" not in cache_db.table_names():
        cache_db.execute("CREATE TABLE collocations (filename string)")

    existing = pd.read_sql("collocations", cache_db)
    existing_s1 = str(filename_s1_collocated) in existing["filename"].values
    existing_s2 = str(filename_s2_collocated) in existing["filename"].values
    # Don't check for 'filename.exists()', as the file is created when snap STARTS, not when snap finishes,
    # so if the snap fails, the file will still be there.
    # Instead, check if there is a row in the 'collocations' table, and then add these collocation files
    # to the table before returning
    if existing_s1 and existing_s2 and not rebuild:
        print(f"""CACHED COLLOCATION: {s1["uuid"]} and {s2["uuid"]}""")
        return [("S1", filename_s1_collocated), ("S2", filename_s2_collocated)]

    # gpt complains if LD_LIBRARY_PATH is not set
    # for some reason, this works on jupyter, but not from terminal
    if "LD_LIBRARY_PATH" not in os.environ:
        os.environ["LD_LIBRARY_PATH"] = "."
    else:
        parts = os.environ["LD_LIBRARY_PATH"].split(":")
        if "." not in parts:
            os.environ["LD_LIBRARY_PATH"] += ":."

    gpt_file = (
        "gpt_files/gpt_cloud_masks_bands_specified_subset_without_reprojection.xml"
    )
    ROI_subset_string = str(ROI_subset).replace("POLYGON ", "POLYGON")
    if mount:
        gpt_file = str(Path(mount) / gpt_file)
    
    if s1_zip.exists():
        s1_exists = s1_zip
    elif s1_bare.exists():
        s1_exists = s1_bare
    elif s1_safe.exists():
        s1_exists = s1_safe
    else:
        s1_exists = ""
    if s2_zip.exists():
        s2_exists = s2_zip
    elif s2_bare.exists():
        s2_exists = s2_bare
    elif s2_safe.exists():
        s2_exists = s2_safe
    else:
        s2_exists = ""

    if not s1_exists and not s2_exists:
        raise Exception(
            "S1 and S2 products haven't been downloaded yet. Missing {} and {}".format(
                s1_zip, s2_zip
            )
        )
    elif not s1_exists:
        raise Exception(
            "S1 product hasn't been downloaded yet. Missing {}".format(s1_zip)
        )
    elif not s2_exists:
        raise Exception(
            "S2 product hasn't been downloaded yet. Missing {}".format(s2_zip)
        )
    command = [
            "gpt",
            gpt_file,
            "-PS1={}".format(str(s1_exists)),
            "-PS2={}".format(str(s2_exists)),
            "-PCollocate_master={}".format(s2["title"]),
            "-PS1_write_path={}".format(filename_s1_collocated),
            "-PS2_write_path={}".format(filename_s2_collocated),
            "-Pbands_S1={}".format(",".join(config["bands_S1"])),
            "-Pbands_S2={}".format(",".join(config["bands_S2"])),
            "-PROI={}".format(ROI_subset_string),
        ]
    # print("Running: ", ' '.join(command))
    proc_output = subprocess.run( command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    err = proc_output.returncode

    if err:
        output = proc_output.stdout.decode()
        if "out of bounds" in output:
            err_msg = (
                f"""gpt out of bounds error: {s1["uuid"]} and {s2["uuid"]}: {err}"""
            )
            raise senprep.CoordinateOutOfBoundsError(err_msg)
        raise Exception("Collocating: gpt return code %s: %s" % (err, output))

    # Cache successful products
    # here, we save the full filenames of the collocated products to a dictionary
    # that exists in the output location (e.g. something like /var/satellite-data/cache.db
    to_cache = []
    if not existing_s1:
        to_cache.append(f"('{filename_s1_collocated}')")
    if not existing_s2:
        to_cache.append(f"('{filename_s2_collocated}')")

    # The 'EXCEPT .. SELECT' ensures we do not end up with duplicate entries
    if to_cache:
        cache_db.execute(
            """
            INSERT INTO collocations
            VALUES {to_cache}
            EXCEPT
            SELECT filename from collocations
            """.format(
                to_cache=", ".join(to_cache)
            )
        )

    crs = rasterio.open(filename_s1_collocated).crs
    roiutil.export_to_file(ROI_subset, dir_out_for_roi /
                           f"ROI{ROI_no}.geojson", crs)
    return [("S1", filename_s1_collocated), ("S2", filename_s2_collocated)]


@snapper
def s2_with_previous_two_s1_with_same_orbit(products):
    pass
