"""Callback functions for specific Sentinel product sets.

These are to be used by the metaflow SenPrep class, where a configuration
defines which of these callbacks is to be used to generate a list of S1
and S2 products that need downloaded.
"""
# IMPORTS - Std. Lib
from datetime import datetime, timedelta
import math

# IMPORTS - Downloaded packages
import numpy as np
import pandas as pd

# IMPORTS - local
try:
    import roiutil
    import senprep
except:
    from src import roiutil
    from src import senprep

SENTINEL_PRODUCT_FINDERS = set()

# ==================================================
# WARNING
#
# This file should ONLY have callbacks for running
# the sentinelsat product finders.
# All other code should be somewhere else,
# like senprep.py
# ==================================================


def product_finder(func):
    """Adds a function to the list of available snappers.

    Mainly intended for interactive use, so when debugging or running jupyter,
    a user can just check snapper.SENTINEL_SNAPPERS to find out which callbacks
    are available, as snapper.__dict__ lists all imports etc."""
    SENTINEL_PRODUCT_FINDERS.add(func.__name__)
    return func


@product_finder
def s1(config, credentials):
    assert "Finding S1 products NOT IMPLEMENTED"


@product_finder
def s2(config, credentials):
    """Find S2 products.

    Arguments
    ---------
    config : dict containing
        ROI : shapely.shape
            Region of Interest as shapely geometry
        dates : (str, str)
            format yyyymmdd e.g. 20200601 for 1st June 2020
            the START and END dates to query
    credentials : str
        filename for sentinelsat api credentials

    Returns
    -------
    products : List[[S2 product]]
        list of S2-product single-element lists
        e.g. [[S2_1], [S2_2], [S2_3]
    other : dict
        whatever else we want to return
        e.g. %coverage or something
    """
    api = senprep.load_api(credentials)
    roi = roiutil.ROI(config["geojson"])
    other_return_data = dict()

    # Find _ALL_ products within the range, and then filter to a
    start_s2 = senprep.nearest_previous_monday(
        senprep.yyyymmdd_to_date(config["dates"][0])
    )
    end_s2 = senprep.nearest_next_sunday(senprep.yyyymmdd_to_date(config["dates"][1]))
    s2_products = api.to_geodataframe(
        api.query(
            roi.footprint,
            date=(start_s2, end_s2),
            platformname="Sentinel-2",
            cloudcoverpercentage=config.get("cloud_cover", (0, 20)),
            producttype=config.get("s2_producttype", "S2MSI2A"),
        )
    )

    n_weeks = math.ceil((end_s2 - start_s2).days / 7)
    A_WEEK = timedelta(days=7)
    week_boundaries = [
        (start_s2 + n * A_WEEK, start_s2 + (n + 1) * A_WEEK) for n in range(n_weeks)
    ]

    product_map = pd.DataFrame(columns=["week_start", "S2", "ROI"])
    for week_start, week_end in week_boundaries:
        print(week_start, "to", week_end)
        week_start = datetime(week_start.year, week_start.month, week_start.day)
        week_end = datetime(week_end.year, week_end.month, week_end.day)
        mask = (s2_products.beginposition >= week_start) & (
            s2_products.beginposition <= week_end
        )
        s2_thisweek = s2_products[mask]
        s2_thisweek_filtered, _roi_table = senprep.select_S2(
            senprep.sort_S2(s2_thisweek, roi.shape), roi.shape
        )
        product_map.append(s2_thisweek_filtered, ignore_index=True)
    product_list = [
        {"ids": (row.S2), "info": {"roi": row.ROI, "roi_no": int(row.ROI_no)}}
        for _, row in product_map.iterrows()
    ]
    return product_list, other_return_data


@product_finder
def s2_with_previous_s1(config, credentials):
    """Find pairs of S2 and S1 products.

    The S1 product must be in the week before the matching S2 product.

    Arguments
    ---------
    config : dict containing
        ROI : shapely.shape
            Region of Interest as shapely geometry
        dates : (str, str)
            format yyyymmdd e.g. 20200601 for 1st June 2020
            the START and END dates to query
    credentials : str
        filename for sentinelsat api credentials

    Returns
    -------
    products : List of [(S1 product, S2 product), (ROI, ROI_number)]
        list of pairs of S2 and matching S1, with ROI and ROI number
    other : dict
        whatever else we want to return
        e.g. %coverage or something
    """
    api = senprep.load_api(credentials)

    roi = roiutil.ROI(config["geojson"])

    other_return_data = dict()

    # Find _ALL_ products within the range, and then filter to a
    # pair of S2 and S1 per week
    start_s2 = senprep.nearest_previous_monday(
        senprep.yyyymmdd_to_date(config["dates"][0])
    )
    end_s2 = senprep.nearest_next_sunday(senprep.yyyymmdd_to_date(config["dates"][1]))

    s2_products = api.query(
        roi.footprint,
        date=(start_s2, end_s2),
        platformname="Sentinel-2",
        # cloudcoverpercentage MUST be forced as a tuple or the sentinelsat api fails
        cloudcoverpercentage=tuple(config.get("cloudcover", (0, 20))),
        producttype=config.get("s2_producttype", "S2MSI2A"),
    )
    s2_products = api.to_geodataframe(s2_products)

    # Capture s1 products from before and after our S2 window
    # with 7,0 you are strictly looking for S1 BEFORE S2 (e.g. .......S2)
    # with 3,3 you are looking for S1 AROUND S2 (e.g. ...S2...)
    DELTA_DAYS_S1_BEFORE = timedelta(days=3)
    DELTA_DAYS_S1_AFTER = timedelta(days=3)

    start_s1 = start_s2 - DELTA_DAYS_S1_BEFORE
    end_s1 = end_s2 + DELTA_DAYS_S1_AFTER
    s1_products = api.to_geodataframe(
        api.query(
            roi.footprint,
            date=(start_s1, end_s1),
            platformname="Sentinel-1",
            producttype=config.get("s1_producttype", "GRD"),
        )
    )

    n_weeks = math.ceil((end_s2 - start_s2).days / 7)
    week_starts = start_s2 + np.arange(n_weeks) * timedelta(days=7)

    product_map = pd.DataFrame(columns=["week_start", "S1", "S2", "ROI", "ROI_area"])
    used_s1 = []
    for week_start in week_starts:
        week_start = datetime(week_start.year, week_start.month, week_start.day)
        week_end = week_start + timedelta(days=6)
        week_end = datetime(week_end.year, week_end.month, week_end.day)
        mask = (s2_products.beginposition >= week_start) & (
            s2_products.beginposition < week_end
        )
        s2_thisweek = s2_products[mask]
        s2_thisweek_filtered, roi_table = senprep.select_S2(
            senprep.sort_S2(s2_thisweek, roi.shape), roi.shape
        )
        # TODO add check for 's2_thisweek_filtered.empty'

        # for each product, find S1 products for PREVIOUS week
        week_product_map = []
        for i, (_, this_s2) in enumerate(s2_thisweek_filtered.iterrows()):
            # Get S1 products near this week's S2
            s1_start = this_s2.beginposition - DELTA_DAYS_S1_BEFORE
            s1_end = this_s2.beginposition + DELTA_DAYS_S1_AFTER
            mask_s1 = (s1_products.beginposition >= s1_start) & (
                s1_products.beginposition <= s1_end
            )
            s1_thisweek = s1_products[mask_s1]
            # Sort s1 products to get 100% coverage, and filter to best matches
            s1_thisweek_filtered, roi_table_secondary = senprep.select_S1(
                senprep.sort_S1(s1_thisweek, roi.shape), roi_table[i]
            )

            # TODO check for s1_thisweek_filtered.empty
            # For each S1, pair it with this week's s2
            for row_num, (_, this_s1) in enumerate(s1_thisweek_filtered.iterrows()):
                this_roi = roi_table_secondary[row_num]
                week_product_map.append(
                    (
                        week_start,
                        this_s1,
                        this_s2,
                        this_roi,
                        this_roi.area / roi.shape.area * 100,
                    )
                )
        week_product_map = pd.DataFrame(
            week_product_map, columns=["week_start", "S1", "S2", "ROI", "ROI_area"]
        )
        week_product_map.sort_values(by=["ROI_area"], ascending=False, inplace=True)
        week_product_map["ROI_no"] = np.arange(1, week_product_map.shape[0] + 1)
        product_map = product_map.append(week_product_map, ignore_index=True)
        # --- end of loop over S1 for this s2
    # --- end of loop over s2 (week boundaries)

    product_tuples = []
    for _, row in product_map.iterrows():
        product_tuples.append(
            {
                "ids": (row.S1, row.S2),
                "info": {"roi": row.ROI, "roi_no": int(row.ROI_no)},
            }
        )
    return product_tuples, other_return_data


@product_finder
def s2_with_previous_two_s1_with_same_orbit(config, credentials):
    """Find S2,S1,S1_old.

    The S3 must have the same 'relative orbit', which is ~12days flyby time."""
    assert "Finding S2 products with 2 previous S1 with same orbit NOT IMPLEMENTED"

    """Find pairs of S2 and S1 and S1_old products.

    The S1 product must be in the week before the matching S2 product.

    Arguments
    ---------
    config : dict containing
        ROI : shapely.shape
            Region of Interest as shapely geometry
        dates : (str, str)
            format yyyymmdd e.g. 20200601 for 1st June 2020
            the START and END dates to query
    credentials : str
        filename for sentinelsat api credentials

    Returns
    -------
    products : List of [(S1 product, S1_old product, S2 product), (ROI, ROI_number)]
        list of pairs of S2 and matching S1, S1_old, with ROI and ROI number
    other : dict
        whatever else we want to return
        e.g. %coverage or something
    """
    api = senprep.load_api(credentials)

    roi = roiutil.ROI(config["geojson"])

    other_return_data = dict()

    # Find _ALL_ products within the range, and then filter to a
    # pair of S2 and S1 per week
    start_s2 = senprep.nearest_previous_monday(
        senprep.yyyymmdd_to_date(config["dates"][0])
    )
    end_s2 = senprep.nearest_next_sunday(senprep.yyyymmdd_to_date(config["dates"][1]))
    s2_products = api.to_geodataframe(
        api.query(
            roi.footprint,
            date=(start_s2, end_s2),
            platformname="Sentinel-2",
            cloudcoverpercentage=config.get("cloud_cover", (0, 20)),
            producttype=config.get("s2_producttype", "S2MSI2A"),
        )
    )

    # Capture s1 products from before and after our S2 window
    # with 7,0 you are strictly looking for S1 BEFORE S2 (e.g. .......S2)
    # with 3,3 you are looking for S1 AROUND S2 (e.g. ...S2...)
    DELTA_DAYS_S1_BEFORE = timedelta(days=3)
    DELTA_DAYS_S1_AFTER = timedelta(days=3)

    start_s1 = start_s2 - DELTA_DAYS_S1_BEFORE
    end_s1 = end_s2 + DELTA_DAYS_S1_AFTER
    s1_products = api.to_geodataframe(
        api.query(
            roi.footprint,
            date=(start_s1, end_s1),
            platformname="Sentinel-1",
            producttype=config.get("s1_producttype", "SLC"),
            sensoroperationalmode="IW",
        )
    )

    n_weeks = math.ceil((end_s2 - start_s2).days / 7)
    A_WEEK = timedelta(days=7)
    week_boundaries = [
        (start_s2 + n * A_WEEK, start_s2 + (n + 1) * A_WEEK) for n in range(n_weeks)
    ]

    product_map = pd.DataFrame(
        columns=["week_start", "S1", "S1_old", "S2", "ROI", "ROI_area"]
    )
    used_s1 = []
    used_s1_old = []
    # print(s2_products.beginposition)
    for week_start, week_end in week_boundaries:
        # print(week_start, 'to', week_end)
        week_start = datetime(week_start.year, week_start.month, week_start.day)
        week_end = datetime(week_end.year, week_end.month, week_end.day)
        mask = (s2_products.beginposition >= week_start) & (
            s2_products.beginposition <= week_end
        )
        s2_thisweek = s2_products[mask]
        s2_thisweek_filtered, ROI_table = senprep.select_S2(
            senprep.sort_S2(s2_thisweek, roi.shape), roi.shape
        )
        # print("\nWeek {week_start} to {week_end} has {pct}% coverage".format(
        #     week_start=week_start,
        #     week_end=week_end,
        #     pct=s2_thisweek_filtered.Percent_area_covered.sum()
        # ))
        # TODO add check for 's2_thisweek_filtered.empty'

        # for each product, find S1 products for PREVIOUS week
        week_product_map = []
        # for _, row in s2_thisweek_filtered.iterrows():
        #     print(row.beginposition, row.title)
        # print(s2_thisweek_filtered[['beginposition', 'title']])
        for i, (_, this_s2) in enumerate(s2_thisweek_filtered.iterrows()):
            # Get S1 products near this week's S2
            s1_start = week_start - DELTA_DAYS_S1_BEFORE
            s1_end = week_start + DELTA_DAYS_S1_AFTER
            # print(this_s2.beginposition, 'implies', s1_start, 'to', s1_end)
            mask_s1 = (s1_products.beginposition >= s1_start) & (
                s1_products.beginposition <= s1_end
            )
            s1_thisweek = s1_products[mask_s1]
            # Sort s1 products to get 100% coverage, and filter to best matches
            s1_thisweek = s1_thisweek[~s1_thisweek.title.isin(used_s1)]
            s1_thisweek_filtered, ROI_table_secondary = senprep.select_S1(
                senprep.sort_S1(s1_thisweek, roi.shape), ROI_table[i]
            )
            # print("\t{i} of {n_s2} this week's s2 - {n_s1} S1 products - {pct}% coverage".format(
            #     i=i,
            #     n_s2=s2_thisweek_filtered.shape[0],
            #     n_s1=s1_thisweek_filtered.shape[0],
            #     pct=s1_thisweek_filtered.Percent_area_covered.sum()
            # ))

            # TODO check for s1_thisweek_filtered.empty
            # For each S1, pair it with this week's s2
            for row_num, (_prod_id, this_s1) in enumerate(
                s1_thisweek_filtered.iterrows()
            ):
                this_roi = ROI_table_secondary[row_num]

                s1_old_date = this_s1["beginposition"].to_pydatetime()

                start_old_s1 = date(
                    s1_old_date.year, s1_old_date.month, s1_old_date.day
                ) - timedelta(12)
                end_old_s1 = start_old_s1 + timedelta(1)

                s1_old_products = api.to_geodataframe(
                    api.query(
                        date=(start_old_s1, end_old_s1),
                        platformname=this_s1["platformname"],
                        relativeorbitnumber=this_s1["relativeorbitnumber"],
                        sensoroperationalmode=this_s1["sensoroperationalmode"],
                        producttype=this_s1["producttype"],
                        slicenumber=this_s1["slicenumber"],
                    )
                )
                this_s1_old = s1_old_products.iloc[0].squeeze()
                week_product_map.append(
                    (
                        week_start,
                        this_s1,
                        this_s1_old,
                        this_s2,
                        this_roi,
                        this_roi.area / roi.shape.area * 100,
                    )
                )
                used_s1.append(this_s1.title)
                used_s1_old.append(this_s1_old.title)
        week_product_map = pd.DataFrame(
            week_product_map,
            columns=["week_start", "S1", "S1_old", "S2", "ROI", "ROI_area"],
        )
        week_product_map.sort_values(by=["ROI_area"], ascending=False, inplace=True)
        week_product_map["ROI_no"] = np.arange(1, week_product_map.shape[0] + 1)
        product_map = product_map.append(week_product_map, ignore_index=True)
    product_tuples = [
        {
            "ids": (row.S1, row.S1_old, row.S2),
            "info": {"roi": row.ROI, "roi_no": int(row.ROI_no)},
        }
        for _, row in product_map.iterrows()
    ]
    return product_tuples, other_return_data
