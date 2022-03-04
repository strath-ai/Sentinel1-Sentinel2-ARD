#!/usr/bin/env python3
"""Prepare sentinel data for a given ROI between dates.

Usage:
    senprep.py create [-c]
    senprep.py list --config=CONFIG_FILE [options]
    senprep.py download --config=CONFIG_FILE [options]
    senprep.py process --config=CONFIG_FILE [options]
    senprep.py download_process --config=CONFIG_FILE [options]

Commands:
    create                Create or clone an existing configuration file
    list                  List SENTINEL products that match a configuration
    download              Download SENTINEL products that match a configuration
    process               Run processing on already-downloaded products
    download_process      Run the full download+processing pipeline

Options:
    CREATE command (configurations)
        -c                               Let user paste geojson, rather than ask for filename
    LIST, DOWNLOAD, PROCESS, DOWNLOAD_PROCESS commands (satellite pipeline)
       --config CONFIG_FILE             File containing region file to preprocess
       --credentials CREDENTIAL_FILE    File containing sentinel API credentials [default: credentials.json]
       --rebuild                        Rebuild products
       --full_collocation               Whether collocate the whole product or only the roi
       --skip_week                      Skip all weeks that do not yield products covering complete ROI
       --primary primary_PRODUCT        Select primary product S1 or S2 [default: S2]
       --skip_secondary                 Skip the listing and processing of secondary product
       --external_bucket                Will check LTA products from AWS, Google or Sentinelhub
       --available_area                 Will list part of an ROI that matches the required specifications
       --multitemporal                  Will include old S1
       --S1_SLC                         Use S1 SLC instead of GRD products
       --secondary_time_delta DAYS      Use delta time between primary and secondary products [default: 3]
       --primary_prod_frequency DAYS    Frequency in days between primary products [default: 7]
       --cloud_mask_filtering           Download cloud masks to determine whether ROI is cloud covered
"""
from IPython import display
from datetime import date, timedelta, datetime
from descartes.patch import PolygonPatch
from docopt import docopt
from functools import partial
from osgeo import gdal
from pathlib import Path
from rasterio.mask import mask
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt, sentinel
from shapely.geometry import Polygon, MultiPolygon, shape
from shapely.ops import transform, cascaded_union
from subprocess32 import check_output
import geopandas as gpd
import getpass
import json
import logging
import math
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import pdb
import pyperclip
import pyproj
import rasterio as rio
import shutil
import subprocess
import tempfile
import time
import utm
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)
import requests
from google.cloud import storage

# TODO -- use the distinct plot library, rather than re-exporting functions
try:
    from src import configutil, roiutil, sen_plot
    from src.sen_plot import plot_ROI, plot_Stiles_plus_ROI, plot_S1S2tiles_plus_ROI
    from src.download import *
except Exception as E:
    import configutil, roiutil, sen_plot
    from sen_plot import plot_ROI, plot_Stiles_plus_ROI, plot_S1S2tiles_plus_ROI
    from download import *


SENTINEL_ROOT = "/var/satellite-data/"
SENTINEL_STORAGE_PATH = "/var/satellite-data/Sentinel_Patches/"
DEBUG = False

# TODO - remove global variables
# Global variables make it difficult to maintain code.
# Sentinel root and storage path are designed to be modified INSIDE USER CODE if necessary
# (e.g. import senprep... senprep.SENTINEL_ROOT = <some_server_specific_path>)
# Changing the global variables means stuff like docker can easily screw up, as it expects sentinel_root
# to be something specific in order to mount to the correct location.
S1_boundary_buffer = -9000
##### SCl masks
NO_DATA = 0
SATURATED_OR_DEFECTIVE = 1
DARK_AREA_PIXELS = 2
CLOUD_SHADOWS= 3
VEGETATION = 4
NOT_VEGETATED = 5
WATER = 6
UNCLASSIFIED = 7
CLOUD_MEDIUM_PROBABILITY = 8
CLOUD_HIGH_PROBABILITY = 9
THIN_CIRRUS = 10
SNOW = 11

# TODO -- Priti, please don't use the main src for personal customisation,
# as it'll mess up other people's code.
# from matplotlib import colors
# cmap = colors.ListedColormap(['r','w','k','gray','g','y','b', 'pink','aquamarine', 'greenyellow', 'deepskyblue'])
# pd.set_option('display.max_colwidth', None)



def multipolygon_to_polygon(mpoly):
    '''
    Returns the largest polygon present (based on area) in multipolygon to give a polygon
    '''
    area_list = [x.area for x in mpoly]
    area_array = np.array(area_list)
    max_area_polygon_no = np.argmax(area_array)
    poly = mpoly[max_area_polygon_no]
    return poly


def get_utm_crs(lat, lon):
    utm_code =utm.from_latlon(lat, lon) ## (latitude, longitude)
    if utm_code[3]>='N':
        hemisphere ='N'
    else:
        hemisphere ='S'
    crs = pyproj.CRS("WGS 84 / UTM Zone "+str(utm_code[2])+hemisphere)
    return crs


class NoGoogleAuthError(Exception):
    pass


def authenticate_google_cloud(credentials_file=None):
	key_file = None
	auth_list = subprocess.run("gcloud auth list".split(" "), capture_output=True).stdout
	active = [l for l in auth_list.splitlines() if l.startswith(b'* ')]
	if active:
		return

	if credentials_file:
		key_file = str(credentials_file)
	elif os.path.exists("credentials_gs.json"):
		key_file = "credentials_gs.json"
	else:
		raise NoGoogleAuthError("No credentials passed, credentials_gs.json, or active account.")
	subprocess.run([
		"gcloud",
		"auth",
		"activate-service-account",
		"--key-file={}".format(key_file)
	])




def get_roi_cloud_cover(s2_product, ROI_footprint, print_plot = False):
    productname = s2_product['title']
    utm = productname[39:41]
    latb = productname[41:42]
    square = productname[42:44]
    if not os.path.exists('./temp'):
        os.mkdir('./temp')
    save_filename = Path('./temp/{}.gml'.format(productname))

    if not save_filename.exists():
        proc_output = subprocess.run(
                [   "gsutil",
                     "-m",
                     "cp",
                     "-r",
                     "gs://gcp-public-data-sentinel-2/L2/tiles/{}/{}/{}/{}.SAFE/GRANULE/*/*/MSK_CLOUDS_B00.gml".format(utm,latb,square,productname),
                     "{}".format(save_filename)
                ],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                )
    try:
         cloud_data = gpd.read_file(save_filename, layer=0)
    except ValueError:
        ### If cloud file empty
        ROI_centroid = ROI_footprint.centroid.coords
        utm_crs = get_utm_crs(list(ROI_centroid)[0][1], list(ROI_centroid)[0][0])
        ROI_gdf = gpd.GeoDataFrame([1], geometry=[ROI_footprint], crs="epsg:4326")
        ROI_gdf_utm = ROI_gdf.to_crs(utm_crs)
        wgs84 = pyproj.Proj(init="epsg:4326")
        utm = pyproj.Proj(utm_crs)
        project = partial(pyproj.transform, wgs84, utm)
        S2_utm = transform(project, s2_product['geometry'])
        S2_utm_area = S2_utm.area
        cloud_free_area = ((ROI_gdf_utm.geometry[0].intersection(S2_utm)).area)/1e6 ### in km2
        return cloud_free_area

    cloud_data['valid']=cloud_data.geometry.is_valid
    ### To turn invalid geometry to valid geometry
    cloud_data.geometry[cloud_data['valid']==False] = cloud_data.geometry[cloud_data['valid']==False].buffer(0)
    ROI_gdf = gpd.GeoDataFrame([1], geometry=[ROI_footprint], crs="epsg:4326")
    ROI_gdf_utm = ROI_gdf.to_crs(cloud_data.crs)
    cloud_clipped = gpd.clip(cloud_data, ROI_gdf_utm)

    if cloud_clipped.empty == False:
        clipped_filename = Path('./temp/{}_clipped.gml'.format(productname))

        cloud_clipped.to_file(clipped_filename, driver="GeoJSON")
    wgs84 = pyproj.Proj(init="epsg:4326")
    utm = pyproj.Proj(init=str(cloud_data.crs))
    project = partial(pyproj.transform, wgs84, utm)
    S2_utm = transform(project, s2_product['geometry'])

    if print_plot:
        cloud_clipped.plot(column='gml_id', legend=True)
        ROI_S2_bounds = (ROI_gdf_utm.geometry[0].intersection(S2_utm)).bounds
        plt.xlim([ROI_S2_bounds[0],ROI_S2_bounds[2]])
        plt.ylim([ROI_S2_bounds[1],ROI_S2_bounds[3]])
        plt.title(s2_product['title'])

    S2_utm_area = S2_utm.area
    cloud_free_area = ((ROI_gdf_utm.geometry[0].intersection(S2_utm)).area - cascaded_union(cloud_clipped.geometry).area)/1e6 ### in km2
#     print("cloud_free_area",cloud_free_area )
    return cloud_free_area


def get_scl_cloud_mask(s2_product, ROI_footprint, print_plot = False):
    productname = s2_product['title']
    utm = productname[39:41]
    latb = productname[41:42]
    square = productname[42:44]
    if not os.path.exists('./temp'):
        os.mkdir('./temp')

    save_filename = Path('./temp/{}.jp2'.format(productname))

    if not save_filename.exists():
        proc_output = subprocess.run(
                [   "gsutil",
                     "-m",
                     "cp",
                     "-r",
                     "gs://gcp-public-data-sentinel-2/L2/tiles/{}/{}/{}/{}.SAFE/GRANULE/*/IMG_DATA/R20m/*SCL_20m.jp2".format(utm,latb,square,productname),
                     "{}".format(save_filename)
                ],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                )
    raster_SCL =rio.open(save_filename, driver="JP2OpenJPEG")

    ROI_gdf = gpd.GeoDataFrame([1], geometry=[ROI_footprint], crs="epsg:4326")
    ROI_gdf_utm = ROI_gdf.to_crs(raster_SCL.crs)
    ROI_utm_geometry = ROI_gdf_utm.iloc[0].geometry
    if ROI_utm_geometry.type =="Polygon":
        ROI_utm_geometry = MultiPolygon([ROI_utm_geometry])
    SCL_array, _ = mask(raster_SCL, ROI_utm_geometry  ,all_touched=True, invert=False, crop=True, pad=False, nodata =0)
    cloud_array= (SCL_array>=CLOUD_MEDIUM_PROBABILITY) & (SCL_array<=THIN_CIRRUS)
    if print_plot:
        plt.figure()
        plt.imshow(cloud_array.squeeze(), vmin=0, vmax =1,cmap='gray')
        plt.colorbar()
        plt.title(s2_product['title'])
    cloud_free_area = cloud_array.sum()*400 ## area in meter square

    wgs84 = pyproj.Proj(init="epsg:4326")
    utm = pyproj.Proj(init="epsg:32630")
    project = partial(pyproj.transform, wgs84, utm)
    S2_utm = transform(project, s2_product['geometry'])
    S2_utm_area = S2_utm.area
    cloud_free_area = ((ROI_gdf_utm.geometry[0].intersection(S2_utm)).area - cloud_free_area)/1e6 ### in km2
    return cloud_free_area




def nearest_previous_monday(ddate):
    """Get the Monday before or on a given date.

    Weekday is from 0 (monday) to 6 (sunday)
    so subtract weekday from given date to find the nearest earlier Monday.

    If the passed date IS a Monday, this will be a noop
    i.e. it'll return the same date.
    """
    return ddate - timedelta(days=ddate.weekday())


def nearest_next_sunday(ddate):
    """Get the Sunday after or on a given date.

    Weekday is from 0 (monday) to 6 (sunday)
    so subtract weekday from given date to find the nearest earlier Monday.

    If the passed date IS a Sunday, this will be a noop
    i.e. it'll return the same date.
    """
    if ddate.weekday() == 6:
        return ddate
    return ddate - timedelta(days=ddate.weekday()) + timedelta(days=6)


def yyyymmdd_to_date(datestr):
    """Dumb conversion of a yyyymmdd string to date object."""
    year_4d = int(datestr[:4])
    month_2d = int(datestr[4:6])
    day_2d = int(datestr[6:8])
    return date(year_4d, month_2d, day_2d)


def load_api(credentials_json_file_path):
    """Load SentinelAPI with a users credentials."""
    credentials = json.load(open(credentials_json_file_path))
    return SentinelAPI(
        credentials["username"],
        credentials["password"],
        credentials["sentinel_url_endpoint"],
    )


def load_ROI(ROI_path):
    """
    Loads ROI in a shapely geometry
    Parameters:
    ROI_path: Path to the geojson file
    Returns shapely geometry
    """
    with open(ROI_path) as f:
        Features = json.load(f)["features"]

    # IF ROI is a collection of features
    # ROI =(GeometryCollection([shape(feature["geometry"]) for feature in scotland_features]))

    # IF ROI has a single feature
    for feature in Features:
        ROI = shape(feature["geometry"])

    return ROI


def find_S1(ROI_footprint, start_date, end_date, api, **kwargs):
    """
    Finds S1 products given Region of Interest and date range.

    Parameters
    ----------
    ROI_footprint : shapely geometry
        Region of Interest

    start_date, end_date : str
        Dates in format   TODO fix format

    Keyword Arguments
    -----------------
    plot_tiles : bool [default: True]
        Whether to plot S1 product tiles
    verbose : bool [default: False]
        Whether to display information messages

    Returns
    -------
    geopandas.DataFrame of s1 products
    """
    s1_products = api.query(
        ROI_footprint,
        date=(start_date, end_date + timedelta(days=1)),
        platformname="Sentinel-1",
        producttype="GRD",
    )

    s1_products_df = api.to_geodataframe(s1_products)
    if kwargs.get("plot_tiles", True):
        sen_plot.plot_Stiles_plus_ROI(ROI_footprint, s1_products_df, "blue", grid=False)

    n_prod = s1_products_df.shape[0]
    if kwargs.get('verbose', False):
        logging.info(f"Matching Sentinel-1 Products found from ({start_date} to {end_date}): {n_prod}")

    # TODO -- Priti, please explain what this code does, as it's quite unclear
    # and bits and pieces commented out
    ###title =  s1_products_df.iloc[3]['title']#
    ##print('title', title)
    if n_prod >=1:
        first_poly_coords = s1_products_df['geometry'][0].centroid.coords
        crs = get_utm_crs(first_poly_coords[0][1], first_poly_coords[0][0])
        #print('crs', crs)
        utm_geomtery = s1_products_df['geometry'].to_crs(crs=crs)
        buffered_geometry = utm_geomtery.buffer(S1_boundary_buffer)#.simplify(10)
        buffered_geometry_wgs84 = buffered_geometry.to_crs(epsg=4326)#.simplify(0.1)
        ###print("s1_products_df['geometry'][3]", s1_products_df['geometry'][3], buffered_geometry_wgs84 [3])
        s1_products_df['geometry'] = buffered_geometry_wgs84
        if not os.path.exists('./temp'):
            os.mkdir('./temp')

        s1_products_df.to_file('./temp/S1_products.geojson', driver="GeoJSON")

    return s1_products_df

def find_S1_SLC(ROI_footprint, start_date, end_date, api, **kwargs):
    """
    Finds S1 products given Region of Interest and corresponding Sentinel2 product
    geopanda dataframe
    Parameters:
    s2_df: Takes geopanda dataframe returned from Sentinelsat api or sorted version of it
    ROI: Region of Interest as shapely geometry

    Key-word Arguments
    ------------------
    plot_tiles : bool [default: True]
        Whether to plot S1 product tiles
    verbose : bool [default: False]
        Whether to display information messages

    Returns S1 products as a geopanda dataframe
    """
    end_date = end_date + timedelta(days=1)
    date = (start_date, end_date)

    s1_products = api.query(
        ROI_footprint, date=date, platformname="Sentinel-1", producttype="SLC"
    )

    s1_products_df = api.to_geodataframe(s1_products)
    n_prod = s1_products_df.shape[0]
    if n_prod >0:
        if kwargs.get("plot_tiles", True):
             plot_Stiles_plus_ROI(ROI_footprint, s1_products_df, "blue", grid=False)

        if kwargs.get('verbose', False):
           logging.info(f"Matching Sentinel-1 Products found from ({start_date} to {end_date-timedelta(days=1)}): {n_prod}")
#     print("\nMatching Sentinel-1 Products found from",start_date, "to",end_date-timedelta(days=1),': ',n_prod)
    return s1_products_df



def find_S2(ROI_footprint, start_date, end_date, api, **kwargs):
    """Finds S2 products given a Region of Interest.

    Arguments
    ---------
    ROI : shapely.shape
        Region of Interest as shapely geometry
    start_date: str
        format yyyymmdd e.g. 20200601 for 1st June 2020
    end_date: str
        format yyyymmdd e.g. 20200601 for 1st June 2020
    api : sentinelsat api

    Keyword-Arguments
    -----------------
    cloud_cover : tuple (int int)
        Lower and upper bound of acceptable cloud cover
    cloud_mask_filtering : bool [default: False]
        Whether to check that cloud cover is over ROI or not.

    Returns
    -------
    s2_products_df : geopandas.DataFrame
    """
    # TODO -- Priti, why add the timedelta here, instead of adding timedelta BEFORE calling find_S2?
    # We should document why we have some hard-coded numbers (like timedelta(days=1)), as it's hard to
    # remember why code is a certain way.
    end_date = end_date + timedelta(days=1)
    cloud_cover = tuple(kwargs.get("cloud_cover", (0, 20)))
    s2_products = api.query(
        ROI_footprint,
        date=(start_date, end_date),
        platformname="Sentinel-2",
        cloudcoverpercentage=cloud_cover,
        producttype="S2MSI2A",
    )

    s2_products_df = api.to_geodataframe(s2_products)
    n_prod = s2_products_df.shape[0]

    print(
        f"\nMatching Sentinel-2 Products found from {start_date} to {end_date}: {n_prod}"
    )

    # TODO -- This code is quite indented and difficult to read...tidy
    # TODO -- Priti...why are we creating temporary geojson?
    if n_prod >0:
        cloud_roi_table = []
        cloud_roi_scl_table= []
        if cloud_mask_filtering:
             for i in range(0, s2_products_df.shape[0]):
                 cloud_roi = get_roi_cloud_cover(s2_products_df.iloc[i], ROI_footprint, print_plot = False)
                 cloud_roi_scl = get_scl_cloud_mask(s2_products_df.iloc[i], ROI_footprint, print_plot = False)
                 cloud_roi_table.append(cloud_roi)
                 cloud_roi_scl_table.append(cloud_roi_scl)
             s2_products_df['ROI_cloud_free_area'] = cloud_roi_table
             s2_products_df['SCL_area'] = cloud_roi_scl_table



             print( (s2_products_df[['title','ROI_cloud_free_area', 'SCL_area','cloudcoverpercentage']]).to_string(index=False))

        else:
            print( (s2_products_df[['title', 'beginposition','cloudcoverpercentage']]).to_string(index=False))
        if not os.path.exists('./temp'):
            os.mkdir("./temp")
        s2_products_df.to_file('./temp/S2_products.geojson', driver="GeoJSON")

    return s2_products_df


def find_S1_IW(ROI_footprint, start_date, end_date, api, **kwargs):
    """
    Finds SLC IW S1 products given Region of Interest and timeframe
    geopanda dataframe
    Parameters
    ----------
    ROI: shapely geometry
        Region of Interest
    start_date, end_date: date format
    api: sentinelsat api
    Keyword Arguments
    -----------------
    plot_tiles : bool [default: True]
        Whether to plot S1 product tiles
    Returns
    -------
    geopandas.DataFrame of s1 products
    """
    s1_products = api.query(
        ROI_footprint,
        date=(start_date, end_date),
        platformname="Sentinel-1",
        sensoroperationalmode="IW",
    )

    s1_products_df = api.to_geodataframe(s1_products)
    if kwargs.get("plot_tiles", True):
        sen_plot.plot_Stiles_plus_ROI(ROI_footprint, s1_products_df, "blue", grid=False)
    n_prod = s1_products_df.shape[0]
    print(
        f"\nMatching Sentinel-1 Products found from {start_date} to {end_date}: {n_prod}"
    )

    return s1_products_df


def find_S1_IW_old(s1_products_df, api, **kwargs):
    """
    Finds old S1 product given ONE current SAR IW product dataframe and Region of Interest
    Parameters
    ----------
    s1_products_df: geopandas.DataFrame
         Returned from Sentinelsat api
    ROI: shapely geometry
        Region of Interest
    api: sentinel api
    Keyword Arguments
    -----------------
    plot_tiles : bool [default: True]
        Whether to plot S1 product tiles
    Returns
    -------
    geopandas.DataFrame of one s1 product
    """

    s1_old_date = s1_products_df.beginposition.to_pydatetime()

    # TODO -- Priti, can you add comments explaining the various timedeltas?
    # It's hard to go back and understand this code.
    start_old_s1 = date(s1_old_date.year,s1_old_date.month,s1_old_date.day) - timedelta(13)
    end_old_s1 = start_old_s1 + timedelta(2) + timedelta(days=1) ## Timedelta 1 added due to sentinel end_date format
    slicenumber = int(s1_products_df.slicenumber)
    s1_old_products = api.query(s1_products_df['geometry'],
                        date = (start_old_s1,end_old_s1),
                        platformname = s1_products_df.platformname,
                        relativeorbitnumber = s1_products_df.relativeorbitnumber,
                        sensoroperationalmode = s1_products_df.sensoroperationalmode,
                        producttype =  s1_products_df.producttype,
                        slicenumber = slicenumber,
                        polarisationmode= s1_products_df.polarisationmode)

    s1_old_products_df = api.to_geodataframe(s1_old_products)

    n_prod = s1_old_products_df.shape[0]
#     print(f"\nCurrent S1 date {s1_old_date}")
#     print(s1_products_df)
#     print(f"\nMatching Old Sentinel-1 Products found from {start_old_s1} to {end_old_s1-timedelta(days=1)}: {n_prod}")
    return s1_old_products_df


def sort_sentinel_products(products, ROI, sorting_params, sorting_params_ascending):
    """
    Sort Sentinel tiles based on the common ROI geometry
    Returns panda dataframe with first column as product id and second column as the
    percentage overlap area with ROI after sorting

    Parameters
    ----------
    products: geopandas.geodataframe.GeoDataFrame
        returned from Sentinelsat api
    ROI: shapely.geometry.multipolygon.MultiPolygon
        Region of interest to check for common geometry
    sorting_params : list of str
        Which parameters to sort by
    sorting_params_ascending : list of bool
        Whether the `sorting_params` should be ascending

    Returns
    -------
    panda dataframe
    """
    column_label = ["Product_id", "overlap_area"]  # overlap_area is the absolute area
    table = []

    for i in range(0, products.shape[0]):
        s_geometry = products["geometry"][i]
        if s_geometry.intersects(ROI):
            common_area = (s_geometry.intersection(ROI)).area
        else:
            common_area = 0

        data = [products.index[i], common_area]
        table.append(data)

    dataframe = pd.DataFrame(table, columns=column_label)
    sorted_dataframe = dataframe.sort_values(by=["overlap_area"], ascending=False)

    # Rearranging products as per the "Percent_overlap"
    products_sorted = products.reindex(sorted_dataframe["Product_id"])
    products_sorted["overlap_area"] = list(sorted_dataframe["overlap_area"])
    products_sorted = products_sorted.sort_values(
        by=sorting_params, ascending=sorting_params_ascending
    )
    products_sorted = products_sorted[products_sorted["overlap_area"] > 0.0]
    return products_sorted


def sort_S1(s_products_df, ROI, **kwargs):
    """
    Sort Sentinel tiles based on the common ROI geometry
    Returns panda dataframe with first column as product id and second column as the
    percentage overlap area with ROI after sorting

    Parameters
    ----------
    s_product_df: geopandas.geodataframe.GeoDataFrame returned from Sentinelsat api
    ROI: shapely.geometry.multipolygon.MultiPolygon

    Keyword Arguments
    -----------------
    sorting_params : list of str
        Which parameters to sort by (Default ["overlap_area","beginposition"])
    sorting_params_ascending : list of bool
        Whether the `sorting_params` should be ascending (Default [False, True])

    Returns
    -------
        panda dataframe
    """
    sorting_params = kwargs.get("sorting_params", ["overlap_area", "beginposition"])
    sorting_params_ascending = kwargs.get("sorting_params_ascending", [False, True])
    with pd.option_context('mode.chained_assignment', None):
        s_products_df["overlap_area"] = s_products_df["geometry"].intersection(ROI).area  # in km2
    s_products_df_sorted = s_products_df.sort_values(by=sorting_params, ascending=sorting_params_ascending)
    s_products_df_sorted = s_products_df_sorted[s_products_df_sorted["overlap_area"] > 0.0]
    return s_products_df_sorted


def sort_S2(s_products_df, ROI, **kwargs):
    """
    Sort Sentinel2 tiles based on the common ROI geometry
    Returns panda dataframe with first column as product id and second column as the
    percentage overlap area with ROI after sorting

    Parameters
    ----------
    s_products_df: geopandas.geodataframe.GeoDataFrame
        returned from Sentinelsat api
    ROI: shapely.geometry.multipolygon.MultiPolygon
        Region of interest to check for common geometry

    Keyword Arguments
    -----------------
    sorting_params : list of str
        Which parameters to sort by
        Default:  ["overlap_area","cloudcoverpercentage","beginposition"]
    sorting_params_ascending : list of bool
        Whether the `sorting_params` should be ascending
        Default: [False, True, True]

    Returns
    -------
    panda dataframe
    """
    sorting_params = kwargs.get(
        "sorting_params", ["overlap_area", "cloudcoverpercentage", "beginposition"]
    )
    sorting_params_ascending = kwargs.get(
        "sorting_params_ascending", [False, True, True]
    )
    with pd.option_context('mode.chained_assignment', None):
        s_products_df["overlap_area"] = s_products_df.geometry.intersection(ROI).area   # in km2
    if 'ROI_cloud_free_area' in sorting_params:
        cloud_roi_table = []
        for i in range(0, s_products_df.shape[0]):
            cloud_roi = get_roi_cloud_cover(s_products_df.iloc[i], ROI)
            cloud_roi_table.append(cloud_roi)
        s_products_df['ROI_cloud_free_area'] = cloud_roi_table
    s_products_df_sorted = s_products_df.sort_values(by=sorting_params, ascending=sorting_params_ascending)
    s_products_df_sorted = s_products_df_sorted[s_products_df_sorted["overlap_area"] > 0.0]
    return s_products_df_sorted


def select_sentinel_products(
    products, ROI, sorting_params, sorting_params_ascending, **kwargs
):
    """
    Function to select Sentinel products based on overlap with
    ROI to cover the complete ROI

    Parameters:
    s_products : sorted geopanda dataframe returned from sentinelsat API
                 based on "Percent_overlap" with ROI
    ROI        : Region of Interest
    print_fig  : Boolean value, If passed True, prints all the figures

    Returns list of Sentinel 1 products to download
    """
    if kwargs.get("print_fig", False):
        fig = sen_plot.plot_ROI(ROI, grid=True)
        display.display(fig)
        display.clear_output(wait=True)

    s_products = products
    # s_products = products.query("Percent_overlap > 0.0")

    # "overlap_area" is the actual area selected in the final selection list
    # while the "Percent_area_covered" is the percentage for the covered area

    column_label = ["Product_id", "overlap_area"]
    s_table = []
    Remaining_ROI = ROI
    iteration = 0
    ROI_table = []

    while Remaining_ROI.area >= 1e-10 and s_products.shape[0] > 0:
        iteration = iteration + 1
        s_geometry = s_products.iloc[0]["geometry"]

        overlap = s_geometry.intersection(Remaining_ROI)

        data = [s_products.index[0], s_products.iloc[0]["overlap_area"]]
        s_table.append(data)

        Remaining_ROI = Remaining_ROI.difference(s_geometry)

        ROI_table.append(overlap)

        if kwargs.get("print_fig", False):
            ax.add_patch(PolygonPatch(overlap, fc="red"))
            if iteration == 1:
                display.display(plt.gcf())
                display.clear_output(wait=True)
            else:
                display.display(plt.gcf())
                display.clear_output(wait=True)

        # Now remove this combination
        # first find index of first row
        row1_index = s_products.index[0]
        s_products = s_products.drop(index=row1_index)
        # Resort the the sentinel products

        s_products = sort_S1(
            s_products,
            Remaining_ROI,
            sorting_params=sorting_params,
            sorting_params_ascending=sorting_params_ascending,
        )

    s_final_df = pd.DataFrame(s_table, columns=column_label)
    s_final_products = products.reindex(s_final_df["Product_id"])
    s_final_products["Percent_area_covered"] = list(
        s_final_df["overlap_area"] / ROI.area * 100
    )

    # n_prod = s_final_products.shape[0]
    # print("Selected Sentinel-1 Products : ", n_prod)
    # print(s_final_products[["beginposition", "Percent_area_covered"]])

    return s_final_products, ROI_table


# TODO - These functions are getting messier and messier...each function should try and do
# one specific thing, or they get very difficult to understand and work with
# Can we take the plotting out of this?
# --- maybe, return a list of polygons that this function selects, and then plot separately
def select_S1(s1_products_df, ROI, sorting_params=["overlap_area","beginposition"], sorting_params_ascending =(False,True), **kwargs):
    """
    Function to select Sentinel products based on overlap with
    ROI to cover the complete ROI

    Parameters:
    s_products : sorted geopanda dataframe returned from sentinelsat API
                 based on "Percent_overlap" with ROI
    ROI        : Region of Interest
    print_fig  : Boolean value, If passed True, prints all the figures
    Returns list of Sentinel 1 products to download
    """

    if kwargs.get('print_fig', False):

        fig, ax = plt.subplots(figsize=(7, 7))
        ax.grid(True)

        ax.add_patch(PolygonPatch(ROI, fc="yellow"))
        ax.axis("equal")
        ax.legend(["ROI"])
        ax.set_ylabel("Latitude (degree)")
        ax.set_xlabel("Longitude (degree)")
        display.display(plt.gcf())
        display.clear_output(wait=True)


    column_label = ["Product_id", "overlap_area"] ## "overlap_area" is the actual area selected in the final selection list
                                                                  ## while the "Percent_area_covered" is the percentage for the covered area
    s_table = []

    Remaining_ROI = ROI

    iteration = 0

    ROI_table = []

    ## Resort the the sentinel products
    s_products = sort_S1(s1_products_df, Remaining_ROI,sorting_params=sorting_params, sorting_params_ascending=sorting_params_ascending)
    while Remaining_ROI.area >= 1e-10 and s_products.shape[0] > 0:
        #         time.sleep(3)

        iteration = iteration + 1
        s_geometry = s_products.iloc[0]["geometry"]
        overlap = s_geometry.intersection(Remaining_ROI)
        data = [s_products.index[0], s_products.iloc[0]["overlap_area"]]
        s_table.append(data)
        Remaining_ROI = Remaining_ROI.difference(s_geometry)
        ROI_table.append(overlap)

        if kwargs.get('print_fig', False):
            ax.add_patch(PolygonPatch(overlap, fc="red"))
            if iteration == 1:
                ax.legend(("ROI", "S\u2229ROI",))
                display.display(plt.gcf())
                display.clear_output(wait=True)

            else:
                display.display(plt.gcf())
                display.clear_output(wait=True)

        # Now remove this combination
        # first find index of first row
        row1_index = s_products.index[0]
        s_products = s_products.drop(index=row1_index)

        ## Resort the the sentinel products
        if Remaining_ROI.area >= 1e-10:
            s_products = sort_S1(s_products, Remaining_ROI,sorting_params=sorting_params, sorting_params_ascending=sorting_params_ascending)


    s_final_df = pd.DataFrame(s_table, columns=column_label)
    s_final_products = s1_products_df.reindex(s_final_df["Product_id"])
    s_final_products["Percent_area_covered"] = list(s_final_df["overlap_area"]/ROI.area*100)
    s_final_products["Area_covered"] = list(s_final_df["overlap_area"])
    n_prod = s_final_products.shape[0]
    # if kwargs.get('verbose', False):
    #     logging.info(f"Selected Sentinel-1 Products: {n_prod}")

    # print("Selected Sentinel-1 Products : ",n_prod)
    # print(s_final_products[["beginposition","Percent_area_covered"]])
    if not os.path.exists('./temp'):
        os.mkdir('./temp')

    s_final_products.to_file('./temp/S1_products_selected.geojson', driver="GeoJSON")
    return s_final_products, ROI_table


# TODO - These functions are getting messier and messier...each function should try and do
# one specific thing, or they get very difficult to understand and work with
# Can we take the plotting out of this?
# --- maybe, return a list of polygons that this function selects, and then plot separately
def select_S2(s2_products_df, ROI,sorting_params=["overlap_area","cloudcoverpercentage","beginposition"], sorting_params_ascending =[False,True,True], **kwargs):
    """
    Function to select Sentinel products based on overlap with
    ROI to cover the complete ROI

    Parameters:
    s_products : sorted geopanda dataframe returned from sentinelsat API
                 based on "Percent_overlap" with ROI
    ROI        : Region of Interest
    print_fig  : Boolean value, If passed True, prints all the figures
    Returns list of Sentinel products to download
    """
    if kwargs.get('print_fig', False):
        fig, ax = plt.subplots(figsize=(7, 7))
        ax.grid(True)

        ax.add_patch(PolygonPatch(ROI, fc="yellow"))
        ax.axis("equal")
        ax.legend(["ROI"])
        ax.set_ylabel("Latitude (degree)")
        ax.set_xlabel("Longitude (degree)")
        display.display(plt.gcf())
        display.clear_output(wait=True)


    column_label = ["Product_id", "overlap_area"] ## "overlap_area" is the actual area selected in the final selection list
                                                                  ## while the "Percent_area_covered" is the percentage for the covered area
    s_table = []

    Remaining_ROI = ROI

    iteration = 0

    ROI_table = []
    if 'ROI_cloud_free_area' in sorting_params:
        cloud_free_area_table = []
    ## Resort the list
    s_products =sort_S2(s2_products_df, Remaining_ROI, sorting_params=sorting_params, sorting_params_ascending=sorting_params_ascending)
    while Remaining_ROI.area >= 1e-10 and s_products.shape[0] > 0:
        #         time.sleep(3)
        iteration = iteration + 1
        s_geometry = s_products.iloc[0]["geometry"]
        overlap = s_geometry.intersection(Remaining_ROI)
        if 'ROI_cloud_free_area' in sorting_params:
             cloud_free_area_table.append( s_products.iloc[0]['ROI_cloud_free_area'])
        data = [s_products.index[0], s_products.iloc[0]["overlap_area"]]
        s_table.append(data)

        Remaining_ROI = Remaining_ROI.difference(s_geometry)

        ROI_table.append(overlap)

        if kwargs.get('print_fig', False):
            ax.add_patch(PolygonPatch(overlap, fc="red"))
            if iteration == 1:
                ax.legend(("ROI", "S\u2229ROI",))
                display.display(plt.gcf())
                display.clear_output(wait=True)

            else:
                display.display(plt.gcf())
                display.clear_output(wait=True)

        # Now remove this combination
        # first find index of first row
        row1_index = s_products.index[0]
        s_products = s_products.drop(index=row1_index)

        ## Resort the list
        if Remaining_ROI.area >= 1e-10:
             s_products =sort_S2(s_products, Remaining_ROI, sorting_params=sorting_params, sorting_params_ascending=sorting_params_ascending)
    s_final_df = pd.DataFrame(s_table, columns=column_label)

    s_final_products = s2_products_df.reindex(s_final_df["Product_id"])
    s_final_products["Percent_area_covered"] = list(s_final_df["overlap_area"]/ROI.area*100) ###Multiply by 1e6 to get ROI_utm area in km2 from m2
    s_final_products["Area_covered"] = list(s_final_df["overlap_area"])
    if 'ROI_cloud_free_area' in sorting_params:
             s_final_products['ROI_cloud_free_area'] =  cloud_free_area_table
    n_prod = s_final_products.shape[0]

    # if kwargs.get('verbose', False):
    #     logging.info(f"Selected Sentinel-2 Products: {n_prod}")
    if not os.path.exists('./temp'):
        os.mkdir("./temp")

    s_final_products.to_file('./temp/S2_products_selected.geojson', driver="GeoJSON")
    # print("Selected Sentinel-2 Products : ",n_prod)
    # if 'ROI_cloud_free_area' in sorting_params:
    #     print(s_final_products[["title","beginposition","ROI_cloud_free_area","cloudcoverpercentage", "Area_covered", "Percent_area_covered"]].to_string(index=False))
    # else:
    #     print(s_final_products[["title","beginposition","cloudcoverpercentage","overlap_area", "Percent_area_covered"]].to_string(index=False))
    return s_final_products, ROI_table


def get_s2products_between_dates(start_date, end_date, geojson, cloud_cover, api):
    ROI_footprint = geojson_to_wkt(geojson)
    s2_products_df = gpd.GeoDataFrame()
    start = start_date
    starts = [
        start_date + timedelta(days=week_no * 7)
        for week_no in range(0, math.ceil((end_date - start_date).days / 7))
    ]
    cloud_cover = (0, 20)
    for start in starts:
        end = (start + timedelta(days=6)).strftime("%Y%m%d")
        start = start.strftime("%Y%m%d")
        _products_df = find_S2(ROI_footprint, start, end, api, cloud_cover=cloud_cover)
        s2_products_df = s2_products_df.append(_products_df)
    return sort_S2(s2_products_df, shape(geojson["geometry"]))


def existing_processed_products():
    """Read CSV of products that have already been processed."""
    filename = Path(SENTINEL_ROOT) / "used-products.csv"
    if not filename.exists():
        return None
    dataset = pd.read_csv(filename)
    return dataset


def has_product_been_used(uuid):
    """Check if this product has been used previously."""
    existing = existing_processed_products()
    if not isinstance(existing, pd.DataFrame):
        return False
    has_uuid = not existing.query("uuid == @uuid").empty
    return has_uuid


def mark_product_as_used(*, s1_uuid, s1_date, s2_uuid, s2_date, collocated_folder):
    """Add information about this product to the global 'used product' tracker.

    Arguments MUST be passed as keywords (e.g. uuid=<some_uuid_variable>).
    """
    existing_products = existing_processed_products()
    print("In function mark_product_as_used")
    if not isinstance(existing_products, pd.DataFrame):
        existing_products = pd.DataFrame()
    row = {
        "Processed-date": date.today(),
        "S1-uuid": s1_uuid,
        "S1-date": s1_date,
        "S2-uuid": s2_uuid,
        "S2-date": s2_date,
        "Collocated-folder": collocated_folder,
    }
    existing_products = existing_products.append(row, ignore_index=True)
    filename = Path(SENTINEL_ROOT) / "used-products.csv"
    existing_products.to_csv(filename, index=False)

class CoordinateOutOfBoundsError(Exception):
    """Exception representing known gpt issue 'coordinate out of bounds'."""

    pass

def add_cloud_cover_columns(s2_products_df, ROI_footprint):
    """Add ROI_cloud_free_area and SCL_area to a S2 product dataframe."""
    cloud_roi_table = []
    cloud_roi_scl_table = []
    for i in range(0, s2_products_df.shape[0]):
        cloud_roi = get_roi_cloud_cover(s2_products_df.iloc[i], ROI_footprint, print_plot = False)
        cloud_roi_scl = get_scl_cloud_mask(s2_products_df.iloc[i], ROI_footprint, print_plot = False)
        cloud_roi_table.append(cloud_roi)
        cloud_roi_scl_table.append(cloud_roi_scl)
    s2_products_df['ROI_cloud_free_area'] = cloud_roi_table
    s2_products_df['SCL_area'] = cloud_roi_scl_table
    return s2_products_df



class SentinelPreprocessor:
    """SentinelPreprocessor wraps the Sentinel API with some utility.

    This mainly provides an ability to list products that have not yet been
    downloaded, along with download and preprocess chosen products.
    """

    def __init__(self, config_filename, credentials=None, **kwargs):
        """Init with SentinelAPI credentials.

        Arguments
        ---------
        credentials : str or os.PathLike
            Path to SentinelAPI credentials [default: 'credentials.json']
        config_filename : str or os.PathLike
            Path to SentinelPreprocessor configuration (see `senprep.create_config`)

        Keyword Arguments
        -----------------
        rebuild : bool
            Force rebuilding of products
        """
        if not credentials and Path('credentials.json').exists():
            self.api = load_api('credentials.json')
        elif credentials:
            self.api = load_api(credentials)
        else:
            raise Exception("Either pass credentials file, or have 'credentials.json' in directory")
        self.start = None
        self.end = None
        config = json.load(open(config_filename))
        self.start = config['dates'][0]
        self.end = config['dates'][1]
        self.size = config['size']
        self.overlap = config['overlap']
        self.cloudcover = config['cloudcover']
        self.roi_name = config['name']
        self.roi = roiutil.ROI(config['geojson'])
        self.bands_S1=config['bands_S1']
        self.bands_S2=config['bands_S2']
        self.n_available = None
        self.required_products = dict()
        self.product_map = []
        self.available_s1 = []
        self.available_s2 = []
        self.ran_list = False
        self.rebuild = kwargs.get('rebuild', False)
        self.primary = kwargs.get('primary', None)
        if self.primary=='S2':
            self.secondary='S1'
        elif self.primary=='S1':
            self.secondary='S2'
        self.skip_week = kwargs.get('skip_week', False)
        self.skip_secondary = kwargs.get('skip_secondary', False)
        self.full_collocation = kwargs.get('full_collocation', False)
        self.max_S1_products_per_S2=1 ## Max no of S1 products to be retained per S2 product
        self.S1_delta_time = 3 ## Search S1 products within (S1_delta_time) days of S2
        self.external_bucket = kwargs.get('external_bucket', False)
        self.available_area = kwargs.get('available_area', False)
        self.multitemporal = kwargs.get('multitemporal', False)
        if self.multitemporal:
            self.bands_S1.remove('collocationFlags')
        self.S1_SLC = kwargs.get('S1_SLC', False)
        self.secondary_time_delta = int(kwargs.get('secondary_time_delta', '3')) ## Search secondary products within (S1_delta_time) days of primary product
        self.primary_prod_frequency = int(kwargs.get('primary_prod_frequency', '7')) ## Coverage frequency of primary product
        self.primary_S1_params=["overlap_area","beginposition"]
        self.primary_S1_params_ascending =(False,True)
        self.secondary_S1_params=["overlap_area","abs_time_delta_from_primary_hrs"]
        self.secondary_S1_params_ascending =(False,True)
        self.cloud_mask_filtering = kwargs.get('cloud_mask_filtering', False)
        if self.cloud_mask_filtering:
            self.primary_S2_params = ["overlap_area","SCL_area" , "beginposition"] # "ROI_cloud_free_area"
            self.primary_S2_params_ascending = [False, False, True] ##
            self.secondary_S2_params = ["overlap_area","ROI_cloud_free_area","abs_time_delta_from_primary_hrs"] #
            self.secondary_S2_params_ascending = [False, False, True] ##
        else:
            self.primary_S2_params = ["overlap_area","cloudcoverpercentage","beginposition"] #
            self.primary_S2_params_ascending = [False, True, True] ##
            self.secondary_S2_params = ["overlap_area","cloudcoverpercentage","abs_time_delta_from_primary_hrs"] #
            self.secondary_S2_params_ascending = [False, True, True] ##

    def __make_roi_footprint(self, geojson):
        # Workaround to account for the fact that geojson_to_wkt was
        # working with read_geojson, which requires a file
        f = tempfile.NamedTemporaryFile('w')
        f.write(geojson)
        f.seek(0)
        return geojson_to_wkt(read_geojson(f.name))


    def __repr__(self):
        """Show region information."""
        msg = "SentinelSAT Pre-processor"
        date_msg = "NOT SET"
        roi_msg = "NOT SET"
        available_msg = "NOT SEARCHED YET (run `.find_products()`)"
        date_msg = f"{self.start} till {self.end}"
        roi_msg = f"{self.roi_name}"
        if self.n_available:
            available_msg = f"{self.n_available[0]} S2"
            available_msg += f", {self.n_available[1]} S1"
        msg += f"\n> DATES     | {date_msg}"
        msg += f"\n> ROI       | {roi_msg}"
        msg += f"\n> AVAILABLE | {available_msg}"
        return msg

    def find_primary(self, footprint,  start_date, end_date ):
#         print("Finding primary product")
        if self.primary =='S2':
            primary_products_df =find_S2(footprint, start_date, end_date, self.cloudcover, self.cloud_mask_filtering, self.api)
        elif self.primary =='S1':
            if self.S1_SLC:
                primary_products_df =find_S1_SLC(footprint, start_date, end_date, self.api, plot_tiles=False)
            else:
                primary_products_df =find_S1(footprint, start_date, end_date, self.api, plot_tiles=False)
        return primary_products_df

    def sort_primary(self, primary_products, footprint):
        if self.primary =='S2':
            primary_products_sorted =sort_S2(primary_products, footprint, sorting_params=self.primary_S2_params, sorting_params_ascending=self.primary_S2_params_ascending)
            if self.cloud_mask_filtering:
                print(primary_products_sorted[["title","SCL_area","cloudcoverpercentage", "overlap_area"]].to_string(index=False))
            else:
                print(primary_products_sorted[["title","cloudcoverpercentage", "overlap_area"]].to_string(index=False))
        elif self.primary =='S1':
            primary_products_sorted =sort_S1(primary_products, footprint,sorting_params= self.primary_S1_params, sorting_params_ascending=self.primary_S1_params_ascending)


        return primary_products_sorted

    def select_primary(self, primary_products_sorted, footprint, print_fig):
#         print("Selecting primary product")
        if self.primary =='S2':
            primary_final_df, ROI_table_primary = select_S2(
            primary_products_sorted, footprint, self.primary_S2_params, self.primary_S2_params_ascending
            )
        elif self.primary =='S1':
            primary_final_df, ROI_table_primary = select_S1(
            primary_products_sorted, footprint, self.secondary_S1_params, self.primary_S1_params_ascending
            )
        return primary_final_df, ROI_table_primary

    def find_secondary(self, primary_prod, roi_primary,  plot_tiles):
#         print("Finding secondary product")
        centre_date = primary_prod["beginposition"]
        start_date = centre_date - timedelta(days=self.secondary_time_delta)
        end_date = centre_date + timedelta(days=self.secondary_time_delta)
        if self.primary =='S2':
            if self.S1_SLC:
                secondary_products_df=find_S1_SLC(roi_primary, start_date, end_date, self.api, plot_tiles=False)
            else:
                secondary_products_df=find_S1(roi_primary, start_date, end_date, self.api, plot_tiles=False)
        elif self.primary =='S1':
            secondary_products_df=find_S2(roi_primary, start_date, end_date, self.cloudcover, self.cloud_mask_filtering, self.api)
        return secondary_products_df

    def sort_secondary(self, secondary_products, primary_product, footprint):
        ## Find out the time difference in hours between primary and secondary
        secondary_products["abs_time_delta_from_primary_hrs"]=  abs((secondary_products["beginposition"] -       primary_product["beginposition"]).dt.total_seconds()/3600)
        if self.primary =='S2':
            secondary_products_sorted =sort_S1(secondary_products, footprint,sorting_params= self.secondary_S1_params, sorting_params_ascending=self.secondary_S1_params_ascending)
        elif self.primary =='S1':
            secondary_products_sorted =sort_S2(secondary_products, footprint, sorting_params=self.secondary_S2_params, sorting_params_ascending=self.secondary_S2_params_ascending)
        return secondary_products_sorted

    def select_secondary(self, secondary_products_sorted, primary_product, footprint, print_fig=False):

        if self.primary =='S2':
            secondary_final_df, ROI_table_secondary = select_S1(
            secondary_products_sorted, footprint, self.secondary_S1_params, self.secondary_S1_params_ascending)
        elif self.primary =='S1':
            secondary_final_df, ROI_table_secondary = select_S2(
            secondary_products_sorted, footprint, self.secondary_S2_params, self.secondary_S2_params_ascending)

        return secondary_final_df, ROI_table_secondary

    def find_products(self):
        """
        Query SentinelAPI for matching products.

        """
        find_start_time = datetime.now()
        start_date = nearest_previous_monday(yyyymmdd_to_date(self.start))
        end_date=nearest_next_sunday(yyyymmdd_to_date(self.end))

        print('Initial date, Ending date:',start_date, end_date)

        s2_products_df = gpd.GeoDataFrame()
        starts = [
            start_date + timedelta(days=duration_no * self.primary_prod_frequency)
            for duration_no in range(0, math.ceil((end_date - start_date).days / self.primary_prod_frequency))
        ]

        for start in starts:
            end = (start + timedelta(days=self.primary_prod_frequency-1))
            print('\n \n-----------------------------------------------------------------------')
            print('Duration-start, Duration-end: ', start.strftime("%Y%m%d"),'-', end.strftime("%Y%m%d"))

            week_product_map =[]
            primary_products_df =self.find_primary(self.roi.shape,  start, end)
            if primary_products_df.empty:

                if (self.skip_week == False) and (self.available_area == False):
                    logging.info(f"No matching {self.primary} product found for the week {start} - {end}")
                    user_input = input(f"No matching {self.primary} product found for the week {start} - {end}  Press y to skip this week, n to abort the processing, y_all to skip all weeks not matching required specification: ")
                    if user_input == 'y':
                        continue
                    elif user_input == 'n':
                        raise Exception("Processing aborted")
                    elif user_input == 'y_all':
                        self.skip_week = True
                        continue
                    else:
                        raise Exception("Invalid input, Processing aborted")

                else:
                    continue

            primary_products_sorted = self.sort_primary(primary_products_df, self.roi.shape)

            primary_final_df, ROI_table_primary = self.select_primary(
            primary_products_sorted, self.roi.shape, print_fig=False
            )

            Area_covered=np.sum(primary_final_df["Percent_area_covered"])
            if  (Area_covered<99):
                if (self.skip_week == False):
                    if (self.available_area == False):
                        print(f"Complete ROI is not covered by the primary {self.primary} product, Area covered by {self.primary} products is {Area_covered}%")
                        user_input = input(f"Press 'y' to skip this week, 'y_all' to skip all weeks not matching required specifications, 'a' to process the available part of ROI, 'n' to abort the processing: ")
                        if user_input == 'y':
                            continue
                        elif user_input == 'n':
                            logging.debug(f"Complete ROI is not covered by the primary {self.primary} product, Area covered by {self.primary} products is {Area_covered}% Processing aborted")
                            raise Exception("Processing aborted")
                        elif user_input == 'y_all':
                            self.skip_week = True
                            continue
                        elif user_input == 'a':
                            self.available_area = True
                        else:
                            raise Exception("Invalid input, Processing aborted")
                else:
                    continue

            primary_fig_title = self.primary+' tiles and ROI from '+start.strftime("%Y%m%d")+' to '+end.strftime("%Y%m%d")
#             plot_Stiles_plus_ROI(self.roi.shape, primary_final_df , s_tiles_color="green", grid=False, title =s2_fig_title)

            for i in range(0, primary_final_df.shape[0]):
                primary_prod = primary_final_df.iloc[i]

                if not self.skip_secondary:
                    try:
                        secondary_products_df = self.find_secondary(primary_prod, ROI_table_primary[i],plot_tiles=False)

                    except sentinel.SentinelAPIError:
#                         print('sentinelsat.sentinel.SentinelAPIError')
#                         raise Exception('sentinelsat.sentinel.SentinelAPIError JSON')
                          continue
#                 secondary_fig_title = self.secondary+'tiles and ROI from '+start+' to '+end
#                 plot_S1S2tiles_plus_ROI( ROI_table_primary[i], secondary_final_df ,pd.DataFrame([primary_prod]), grid=False, title=s1_fig_title)
                    if secondary_products_df.empty:

                        if (self.skip_week == False) and (self.available_area == False):
                            logging.info(f"No matching {self.secondary} product found for corresonding {self.primary} for the week {start} - {end}")
                            user_input = input(f"No matching {self.secondary} product found for corresonding {self.primary} for the week {start} - {end} , Press y to skip this week, n to abort the processing, y_all to skip all weeks not matching required specifications: ")
                            if user_input == 'y':
                                continue
                            elif user_input == 'n':
                                raise Exception("Processing aborted")
                            elif user_input == 'y_all':
                                self.skip = True
                                continue
                            else:
                                raise Exception("Invalid input, Processing aborted")
                        else:
                            continue
                    else:
                        secondary_products_sorted = self.sort_secondary(secondary_products_df, primary_prod, ROI_table_primary[i])
                        secondary_final_df,ROI_table_secondary =self.select_secondary(secondary_products_sorted, primary_prod, ROI_table_primary[i], print_fig=False)

                        row_no =0
                        for _, secondary_row in secondary_final_df.iterrows():
                        # print('\t', row['summary'])
                            s1_prod_old = None
                            if self.primary == 'S2':
                                s2_prod = primary_prod
                                s1_prod = secondary_row
                                if self.multitemporal:
                                    s1_prod_old_df = find_S1_IW_old(secondary_row,  self.api)

                                    if s1_prod_old_df.empty:
                                        print('S1 old not found, this ROI is skipped')
                                        continue
                                    elif len(s1_prod_old_df) == 1:
                                        s1_prod_old = s1_prod_old_df.iloc[0]
                                    else:
                                        ref_sensing_date = s1_prod_old_df.iloc[0]['beginposition']
                                        same_sensed_image = True
                                        print(s1_prod_old_df.keys())
                                        for _, s1_old_product in s1_prod_old_df.iterrows():
                                            if s1_old_product.beginposition == ref_sensing_date:
                                                same_sensed_image = True
                                            else:
                                                same_sensed_image = False
                                                break
                                        if same_sensed_image:
                                             s1_prod_old_df = s1_prod_old_df.sort_values(by=["ingestiondate"], ascending=False)
                                             s1_prod_old = s1_prod_old_df.iloc[0]
                                        else:
                                            print(s1_prod_old_df)
                                            raise Exception(" More than 1 S1 old products found, Processing aborted")
                            else:
                                s1_prod = primary_prod
                                s2_prod = secondary_row
                            week_product_map.append((start,s1_prod, s2_prod, s1_prod_old, ROI_table_secondary[row_no], ROI_table_secondary[row_no].area/self.roi.shape.area*100))
                            row_no = row_no + 1

                        week_product_map_df = pd.DataFrame(week_product_map, columns= ['week_start','S1', 'S2', 'S1_old', 'ROI', 'ROI_area'])
                else:
                    if self.primary == 'S2':
                        s2_prod = primary_prod
                        s1_prod = None
                        s1_prod_old = None
                    else:
                        s1_prod = primary_prod
                        s2_prod = None
                        s1_prod_old = None
                    week_product_map.append((start,s1_prod, s2_prod, s1_prod_old, ROI_table_primary[i], ROI_table_primary[i].area/self.roi.shape.area*100))
                    week_product_map_df = pd.DataFrame(week_product_map, columns= ['start','S1', 'S2', 'S1_old', 'ROI', 'ROI_area'])
            week_product_map_df = week_product_map_df.sort_values(by=["ROI_area"], ascending=(False))
            week_product_map_df['ROI_no'] = [n for n in range(1,len(week_product_map_df)+1)]
                    #             week_product_map_df = week_product_map_df.drop(columns=['ROI_area']) ## Area is no longer needed. It was used for sorting

            week_product_map_list=week_product_map_df.values.tolist()
            self.product_map.extend(week_product_map_list)

#         self.available_s2 = total_s2_available
#         self.available_s1 = total_s1_available
#         self.n_available = (len(total_s2_available), len(total_s1_available))
#         msg = f"TOTAL: {len(total_s2_available)} S2 and {len(total_s1_available)} S1 unique products available"
#         msg += f"\nSkipped {s2_products_existing.shape[0]} already-existing S2 products"
#         msg += "\n(and thus their associated s1 products)"
#         logging.info(msg)
        self.ran_list = True
        find_time_taken = datetime.now() - find_start_time
        print("Time taken to find products is",find_time_taken )


    def display_available(self):
        """Display available products."""
        query_start_time = datetime.now()
        if not self.product_map:
            self.find_products()
        print('\n \n-----------------------------------------------------------------------')
        print('Summary')
        print('-----------------------------------------------------------------------')

        if self.primary=='S2':
            print('\nSentinel-2 is the primary product\n')
        elif self.primary=='S1':
            print('\nSentinel-1 is the primary product\n')

        current_start = self.product_map[0][0]
        print('-------------------------')
        print('Duration start date', current_start)

        for (start,s1_df, s2_df, s1_old_df, roi, roi_area, roi_no) in self.product_map:
            if start != current_start:
                current_start = start
                print('-------------------------')
                print('Duration start date', start)

            print('ROI no', roi_no, 'Percentage Area:', roi_area, 'ROI:', roi)
#             print(s1_df.keys())
            if s1_df is not None:
                print("S1 -", s1_df.title, s1_df.slicenumber,s1_df.orbitnumber, s1_df.uuid,s1_df.beginposition,s1_df.Percent_area_covered)
            if s2_df is not None:
                print("S2 -", s2_df.title, s2_df.uuid, s2_df.beginposition,s2_df.Percent_area_covered)
            if s1_old_df is not None:
                print("S1 old -", s1_old_df.title, s1_df.uuid, s1_old_df.beginposition)
        query_time_taken = datetime.now() - query_start_time
        print("Time taken for displaying the products is ",query_time_taken)

    def collocate(self, dir_out_for_roi,ROI_subset, s1_title, s1_id, s1_date, s2_title, s2_id, s2_date):
        """Collocate Sen1 and Sen2 products."""
        s1_zip = str(Path(SENTINEL_ROOT) / f"{s1_title}.zip")
        s2_zip = str(Path(SENTINEL_ROOT) / f"{s2_title}.zip")
        if Path(s1_zip).exists():
            print('S1 Zip file exists')
        else:
            s1_zip =str(Path(SENTINEL_ROOT)/ f"{s1_title}.SAFE/")
            if Path(s1_zip).exists():
                print('S1 Safe file exists')
            else:
                print('S1 File does not exist')
                ## Download the file


        s2_zip = str(Path(SENTINEL_ROOT) / f"{s2_title}.zip")

        if Path(s2_zip).exists():
            print('S2 Zip file exists')
        else:
            s2_zip =str(Path(SENTINEL_ROOT)/ f"{s2_title}.SAFE")
            if Path(s2_zip).exists():
                print('S2 Safe file exists')
            else:
                print('S2 File does not exist')
        imagename = f"S1_{s1_id}_S2_{s2_id}.tif"
        filename_s1_collocated = dir_out_for_roi / "S1" / "Collocated" / imagename
        filename_s2_collocated = dir_out_for_roi / "S2" / "Collocated" / imagename

        filename_s1_collocated.parent.mkdir(exist_ok=True, parents=True)
        filename_s2_collocated.parent.mkdir(exist_ok=True, parents=True)

        ## Combining the bands_S1 and bands_S2 in a string to pass it to the gpt file
        separator = ','
        bands_S1_string=separator.join(self.bands_S1)
        bands_S2_string=separator.join(self.bands_S2)

        logging.debug(f"Colloc fn s1 {filename_s1_collocated} exists? {filename_s1_collocated.exists()}")
        logging.debug(f"Colloc fn s2 {filename_s2_collocated} exists? {filename_s2_collocated.exists()}")

        has_files = filename_s1_collocated.exists() and filename_s2_collocated.exists()
        if has_files and not self.rebuild:
            logging.info(f"Collocation already done for {s1_id} and {s2_id}")
            print(f"Collocation already done for {s1_id} and {s2_id}")
            return filename_s1_collocated, filename_s2_collocated
        logging.info("Collocating might take a long time (...hours) depending upon the size of the area collocated")
        print("Collocating might take a long time (...hours) depending upon the size of the area collocated")
        # gpt complains if LD_LIBRARY_PATH is not set
        # for some reason, this works on jupyter, but not from terminal

        if 'LD_LIBRARY_PATH' not in os.environ:
            os.environ['LD_LIBRARY_PATH'] = '.'

        if not self.full_collocation:

            ROI_subset_string=str(ROI_subset).replace('POLYGON ','POLYGON')
            proc_output = subprocess.run(
                [
                    "gpt",
                    "gpt_files/gpt_cloud_masks_bands_specified_subset_without_reprojection.xml",
                    "-PS1={}".format(s1_zip),
                    "-PS2={}".format(s2_zip),
                    "-PCollocate_master={}".format(s2_title),
                    "-PS1_write_path={}".format(filename_s1_collocated),
                    "-PS2_write_path={}".format(filename_s2_collocated),
                    "-Pbands_S1={}".format(bands_S1_string),
                    "-Pbands_S2={}".format(bands_S2_string),
                    "-PROI={}".format(ROI_subset_string)
                ],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            # stderr=subprocess.DEVNULL # hide gpt's info and warning messages
            )

        else:
            proc_output = subprocess.run(
                    [
                        "gpt",
                        "gpt_files/gpt_cloud_masks_bands_specified.xml",
                        "-PS1={}".format(s1_zip),
                        "-PS2={}".format(s2_zip),
                        "-PCollocate_master={}".format(s2_title),
                        "-PS1_write_path={}".format(filename_s1_collocated),
                        "-PS2_write_path={}".format(filename_s2_collocated),
                        "-Pbands_S1={}".format(bands_S1_string),
                        "-Pbands_S2={}".format(bands_S2_string),
                    ],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            # stderr=subprocess.DEVNULL # hide gpt's info and warning messages
                )

        err = proc_output.returncode

        if err:

            # print(proc_output.stdout.decode())
            print(proc_output)
            if "out of bounds" in proc_output.stdout.decode():
                logging.debug(f"gpt out of bounds error: {s1_id} and {s2_id}")

                raise CoordinateOutOfBoundsError(err)
            raise Exception("Collocating: gpt return code %s " % (err))

            # Add this product to the global list of used-products
        if (self.full_collocation):
            mark_product_as_used(s1_uuid=s1_id, s1_date=s1_date, s2_uuid=s2_id, s2_date=s2_date, collocated_folder = dir_out_for_roi)
        return filename_s1_collocated, filename_s2_collocated

    def collocate_GRD_multitemporal(self, dir_out_for_roi,ROI_subset, s1_title, s1_id, s1_date, s2_title, s2_id, s2_date, s1_old_title, s1_old_id, s1_old_date):
        """Collocate Sen1 and Sen2 products."""
        s1_zip = str(Path(SENTINEL_ROOT) / f"{s1_title}.zip")
        s2_zip = str(Path(SENTINEL_ROOT) / f"{s2_title}.zip")
        s1_old_zip = str(Path(SENTINEL_ROOT) / f"{s1_old_title}.zip")
        if Path(s1_zip).exists():
            print('S1 Zip file exists')
        else:
            s1_zip =str(Path(SENTINEL_ROOT)/ f"{s1_title}.SAFE/")
            if Path(s1_zip).exists():
                print('S1 Safe file exists')
            else:
                print('S1 File does not exist')
                ## Download the file


        s2_zip = str(Path(SENTINEL_ROOT) / f"{s2_title}.zip")

        if Path(s2_zip).exists():
            print('S2 Zip file exists')
        else:
            s2_zip =str(Path(SENTINEL_ROOT)/ f"{s2_title}.SAFE")
            if Path(s2_zip).exists():
                print('S2 Safe file exists')
            else:
                print('S2 File does not exist')
        if Path(s1_old_zip).exists():
            print('S1 old Zip file exists')
        else:
            s1_old_zip =str(Path(SENTINEL_ROOT)/ f"{s1_old_title}.SAFE/")
            if Path(s1_old_zip).exists():
                print('S1 old Safe file exists')
            else:
                print('S1 old File does not exist')
                ## Download the file

        imagename = f"S1_{s1_id}_S2_{s2_id}.tif"
        filename_s1_collocated = dir_out_for_roi / "S1" / "Collocated" / imagename
        filename_s2_collocated = dir_out_for_roi / "S2" / "Collocated" / imagename

        filename_s1_collocated.parent.mkdir(exist_ok=True, parents=True)
        filename_s2_collocated.parent.mkdir(exist_ok=True, parents=True)

        ## Combining the bands_S1 and bands_S2 in a string to pass it to the gpt file
        separator = ','
        bands_S1_string=separator.join(self.bands_S1)
        bands_S2_string=separator.join(self.bands_S2)

        bands_S1_cur_string = bands_S1_string.replace('_S','_S0')
        bands_S1_cur_string = bands_S1_cur_string+','
        bands_S1_old_string = bands_S1_string.replace('_S','_S1')
        bands_S1_combined= bands_S1_cur_string+bands_S1_old_string

        logging.debug(f"Colloc fn s1 {filename_s1_collocated} exists? {filename_s1_collocated.exists()}")
        logging.debug(f"Colloc fn s2 {filename_s2_collocated} exists? {filename_s2_collocated.exists()}")

        has_files = filename_s1_collocated.exists() and filename_s2_collocated.exists()
        if has_files and not self.rebuild:
            logging.info(f"Collocation already done for {s1_id} and {s2_id}")
            print(f"Collocation already done for {s1_id} and {s2_id}")
            return filename_s1_collocated, filename_s2_collocated
        logging.info("Collocating might take a long time (...hours) depending upon the size of the area collocated")
        print("Collocating might take a long time (...hours) depending upon the size of the area collocated")
        # gpt complains if LD_LIBRARY_PATH is not set
        # for some reason, this works on jupyter, but not from terminal

        if 'LD_LIBRARY_PATH' not in os.environ:
            os.environ['LD_LIBRARY_PATH'] = '.'

        if not self.full_collocation:

            ROI_subset_string=str(ROI_subset).replace('POLYGON ','POLYGON')
            proc_output = subprocess.run(
                [
                    "gpt",
                    "gpt_files/GRD_S1_multitemporal.xml",
                    "-PS1={}".format(s1_zip),
                    "-PS2={}".format(s2_zip),
                    "-PS1_old={}".format(s1_old_zip),
                    "-PCollocate_master={}".format(s2_title),
                    "-PS1_write_path={}".format(filename_s1_collocated),
                    "-PS2_write_path={}".format(filename_s2_collocated),
                    "-Pbands_S1={}".format(bands_S1_combined),
                    "-Pbands_S2={}".format(bands_S2_string),
                    "-PROI={}".format(ROI_subset_string)
                ],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            # stderr=subprocess.DEVNULL # hide gpt's info and warning messages
            )

        else:
            raise Exception("Full collocation gpt graph not yet implemented")

        err = proc_output.returncode

        if err:

            # print(proc_output.stdout.decode())
            print(proc_output)
            if "out of bounds" in proc_output.stdout.decode():
                logging.debug(f"gpt out of bounds error: {s1_id} and {s2_id}")

                raise CoordinateOutOfBoundsError(err)
            raise Exception("Collocating: gpt return code %s " % (err))

            # Add this product to the global list of used-products
        if (self.full_collocation):
            mark_product_as_used(s1_uuid=s1_id, s1_date=s1_date, s2_uuid=s2_id, s2_date=s2_date, collocated_folder = dir_out_for_roi)
        return filename_s1_collocated, filename_s2_collocated

    def collocate_SLC_multitemporal(self, dir_out_for_roi,ROI_subset, s1_title, s1_id, s1_date, s2_title, s2_id, s2_date, s1_old_title, s1_old_id, s1_old_date):
        """Collocate Sen1 and Sen2 products."""
        s1_zip = str(Path(SENTINEL_ROOT) / f"{s1_title}.zip")
        s2_zip = str(Path(SENTINEL_ROOT) / f"{s2_title}.zip")
        s1_old_zip = str(Path(SENTINEL_ROOT) / f"{s1_old_title}.zip")
        if Path(s1_zip).exists():
            print('S1 Zip file exists')
        else:
            s1_zip =str(Path(SENTINEL_ROOT)/ f"{s1_title}.SAFE/")
            if Path(s1_zip).exists():
                print('S1 Safe file exists')
            else:
                print('S1 File does not exist')
                ## Download the file


        s2_zip = str(Path(SENTINEL_ROOT) / f"{s2_title}.zip")

        if Path(s2_zip).exists():
            print('S2 Zip file exists')
        else:
            s2_zip =str(Path(SENTINEL_ROOT)/ f"{s2_title}.SAFE")
            if Path(s2_zip).exists():
                print('S2 Safe file exists')
            else:
                print('S2 File does not exist')
        if Path(s1_old_zip).exists():
            print('S1 old Zip file exists')
        else:
            s1_old_zip =str(Path(SENTINEL_ROOT)/ f"{s1_old_title}.SAFE/")
            if Path(s1_old_zip).exists():
                print('S1 old Safe file exists')
            else:
                print('S1 old File does not exist')
                ## Download the file

        imagename = f"S1_{s1_id}_S2_{s2_id}.tif"
        filename_s1_collocated = dir_out_for_roi / "S1" / "Collocated" / imagename
        filename_s2_collocated = dir_out_for_roi / "S2" / "Collocated" / imagename

        filename_s1_collocated.parent.mkdir(exist_ok=True, parents=True)
        filename_s2_collocated.parent.mkdir(exist_ok=True, parents=True)

        s1_date_string = datetime.strptime(s1_date,"%Y%m%d").strftime("%d%b%Y")
        s1_old_date_string = datetime.strptime(s1_old_date,"%Y%m%d").strftime("%d%b%Y")

        ## Combining the bands_S1 and bands_S2 in a string to pass it to the gpt file
        separator = ','
        bands_S1_string=separator.join(self.bands_S1)
        bands_S2_string=separator.join(self.bands_S2)

        bands_S1_extended ='coh_VH_'+s1_date_string+'_'+s1_old_date_string+'_S,coh_VV_'+s1_date_string+'_'+s1_old_date_string+'_S'

        bands_S1_cur_string = bands_S1_string.replace('Sigma0_VH','Sigma0_VH_slv1_'+s1_date_string)
        bands_S1_cur_string = bands_S1_cur_string.replace('Sigma0_VV','Sigma0_VV_slv2_'+s1_date_string)
        bands_S1_old_string = bands_S1_string.replace('Sigma0_VH','Sigma0_VH_slv3_'+s1_old_date_string)
        bands_S1_old_string = bands_S1_old_string.replace('Sigma0_VV','Sigma0_VV_slv4_'+s1_old_date_string)

        bands_S1_combined= bands_S1_cur_string+','+bands_S1_old_string+','+bands_S1_extended

        logging.debug(f"Colloc fn s1 {filename_s1_collocated} exists? {filename_s1_collocated.exists()}")
        logging.debug(f"Colloc fn s2 {filename_s2_collocated} exists? {filename_s2_collocated.exists()}")

        has_files = filename_s1_collocated.exists() and filename_s2_collocated.exists()
        if has_files and not self.rebuild:
            logging.info(f"Collocation already done for {s1_id} and {s2_id}")
            print(f"Collocation already done for {s1_id} and {s2_id}")
            return filename_s1_collocated, filename_s2_collocated
        logging.info("Collocating might take a long time (...hours) depending upon the size of the area collocated")
        print("Collocating might take a long time (...hours) depending upon the size of the area collocated")
        # gpt complains if LD_LIBRARY_PATH is not set
        # for some reason, this works on jupyter, but not from terminal

        if 'LD_LIBRARY_PATH' not in os.environ:
            os.environ['LD_LIBRARY_PATH'] = '.'

        if not self.full_collocation:

            ROI_subset_string=str(ROI_subset).replace('POLYGON ','POLYGON')
            proc_output = subprocess.run(
                [
                    "gpt",
                    "gpt_files/SLC_S1_multitemporal.xml",
                    "-PS1={}".format(s1_zip),
                    "-PS2={}".format(s2_zip),
                    "-PS1_old={}".format(s1_old_zip),
                    "-PCollocate_master={}".format(s2_title),
                    "-PS1_write_path={}".format(filename_s1_collocated),
                    "-PS2_write_path={}".format(filename_s2_collocated),
                    "-Pbands_S1={}".format(bands_S1_combined),
                    "-Pbands_S2={}".format(bands_S2_string),
                    "-PROI={}".format(ROI_subset_string)
                ],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            # stderr=subprocess.DEVNULL # hide gpt's info and warning messages
            )
        else:
            raise Exception("Full collocation gpt graph not yet implemented")

        err = proc_output.returncode

        if err:

            # print(proc_output.stdout.decode())
            print(proc_output)
            if "out of bounds" in proc_output.stdout.decode():
                logging.debug(f"gpt out of bounds error: {s1_id} and {s2_id}")

                raise CoordinateOutOfBoundsError(err)
            raise Exception("Collocating: gpt return code %s " % (err))

            # Add this product to the global list of used-products
        if (self.full_collocation):
            mark_product_as_used(s1_uuid=s1_id, s1_date=s1_date, s2_uuid=s2_id, s2_date=s2_date, collocated_folder = dir_out_for_roi)
        return filename_s1_collocated, filename_s2_collocated


    def snap_s1(self, dir_out_for_roi,ROI_subset, s1_title, s1_id, s1_date):
        """Collocate Sen1 and Sen2 products."""
        s1_zip = str(Path(SENTINEL_ROOT) / f"{s1_title}.zip")
        if Path(s1_zip).exists():
            print('S1 Zip file exists')
        else:
            s1_zip =str(Path(SENTINEL_ROOT)/ f"{s1_title}.SAFE/")
            if Path(s1_zip).exists():
                print('S1 Safe file exists')
            else:
                print('S1 File does not exist')

        imagename = f"S1_{s1_id}.tif"
        filename_s1_collocated = dir_out_for_roi / "S1" / "Collocated" / imagename
        filename_s1_collocated.parent.mkdir(exist_ok=True, parents=True)

        ## Combining the bands_S1 and bands_S2 in a string to pass it to the gpt file

        if 'collocationFlags' in self.bands_S1:
            self.bands_S1.remove('collocationFlags')
        separator = ','
        bands_S1_string=separator.join(self.bands_S1)
        bands_S1_string=bands_S1_string.replace('_S','')

        logging.debug(f"Colloc fn s1 {filename_s1_collocated} exists? {filename_s1_collocated.exists()}")
        has_files = filename_s1_collocated.exists()
        if has_files and not self.rebuild:
            logging.info(f"Collocation already done for {s1_id}")
            print(f"Collocation already done for {s1_id}")
            return filename_s1_collocated
        logging.info("Collocating might take a long time (...hours) depending upon the size of the area collocated")
        print("Collocating might take a long time (...hours) depending upon the size of the area collocated")
        # gpt complains if LD_LIBRARY_PATH is not set
        # for some reason, this works on jupyter, but not from terminal

        if 'LD_LIBRARY_PATH' not in os.environ:
            os.environ['LD_LIBRARY_PATH'] = '.'

        if not self.full_collocation:

            ROI_subset_string=str(ROI_subset).replace('POLYGON ','POLYGON')

            proc_output = subprocess.run(
                [
                    "gpt",
                    "gpt_files/gpt_cloud_masks_bands_specified_subset_without_reprojection_S1.xml",
                    "-PS1={}".format(s1_zip),
                    "-PS1_write_path={}".format(filename_s1_collocated),
                    "-Pbands_S1={}".format(bands_S1_string),
                    "-PROI={}".format(ROI_subset_string)
                ],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            # stderr=subprocess.DEVNULL # hide gpt's info and warning messages
            )

        else:
            proc_output = subprocess.run(
                    [
                        "gpt",
                        "gpt_files/gpt_cloud_masks_bands_specified_S1.xml",
                        "-PS1={}".format(s1_zip),
                        "-PS1_write_path={}".format(filename_s1_collocated),
                        "-Pbands_S1={}".format(bands_S1_string),
                    ],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            # stderr=subprocess.DEVNULL # hide gpt's info and warning messages
                )

        err = proc_output.returncode

        if err:

            # print(proc_output.stdout.decode())
            print(proc_output)
            if "out of bounds" in proc_output.stdout.decode():
                logging.debug(f"gpt out of bounds error: {s1_id}")

                raise CoordinateOutOfBoundsError(err)
            raise Exception("Collocating: gpt return code %s " % (err))
        if self.full_collocation:
            mark_product_as_used(s1_uuid=s1_id, s1_date=s1_date, s2_uuid='None', s2_date='None', collocated_folder = dir_out_for_roi)

        return filename_s1_collocated


    def snap_s2(self, dir_out_for_roi,ROI_subset, s2_title, s2_id, s2_date):
        """Collocate Sen1 and Sen2 products."""
        s2_zip = str(Path(SENTINEL_ROOT) / f"{s2_title}.zip")

        if Path(s2_zip).exists():
            print('S2 Zip file exists')
        else:
            s2_zip =str(Path(SENTINEL_ROOT)/ f"{s2_title}.SAFE")
            if Path(s2_zip).exists():
                print('S2 Safe file exists')
            else:
                print('S2 File does not exist')
        imagename = f"S2_{s2_id}.tif"
        filename_s2_collocated = dir_out_for_roi / "S2" / "Collocated" / imagename
        filename_s2_collocated.parent.mkdir(exist_ok=True, parents=True)

        ## Combining the bands_S2 in a string to pass it to the gpt file
        separator = ','
        bands_S1_string=separator.join(self.bands_S1)
        bands_S2_string=separator.join(self.bands_S2)
        bands_S2_string=bands_S2_string.replace('_M','')
        logging.debug(f"Colloc fn s2 {filename_s2_collocated} exists? {filename_s2_collocated.exists()}")

        has_files = filename_s2_collocated.exists()
        if has_files and not self.rebuild:
            logging.info(f"Collocation already done for {s2_id}")
            print(f"Collocation already done for {s2_id}")
            return filename_s2_collocated
        logging.info("Collocating might take a long time (...hours) depending upon the size of the area collocated")
        print("Collocating might take a long time (...hours) depending upon the size of the area collocated")
        # gpt complains if LD_LIBRARY_PATH is not set
        # for some reason, this works on jupyter, but not from terminal

        if 'LD_LIBRARY_PATH' not in os.environ:
            os.environ['LD_LIBRARY_PATH'] = '.'

        if not self.full_collocation:

            ROI_subset_string=str(ROI_subset).replace('POLYGON ','POLYGON')

            proc_output = subprocess.run(
                [
                    "gpt",
                    "gpt_files/gpt_cloud_masks_bands_specified_subset_without_reprojection_S2.xml",
                    "-PS2={}".format(s2_zip),
                    "-PS2_write_path={}".format(filename_s2_collocated),
                    "-Pbands_S2={}".format(bands_S2_string),
                    "-PROI={}".format(ROI_subset_string)
                ],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            # stderr=subprocess.DEVNULL # hide gpt's info and warning messages
            )

        else:
            proc_output = subprocess.run(
                    [
                        "gpt",
                        "gpt_files/gpt_cloud_masks_bands_specified_S2.xml",
                        "-PS2={}".format(s2_zip),
                        "-PS2_write_path={}".format(filename_s2_collocated),
                        "-Pbands_S2={}".format(bands_S2_string),
                    ],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            # stderr=subprocess.DEVNULL # hide gpt's info and warning messages
                )

        err = proc_output.returncode

        if err:
            # print(proc_output.stdout.decode())
            print(proc_output)
            if "out of bounds" in proc_output.stdout.decode():
                logging.debug(f"gpt out of bounds error: {s2_id}")

                raise CoordinateOutOfBoundsError(err)
            raise Exception("Collocating: gpt return code %s " % (err))
        # Add this product to the global list of used-products
        if self.full_collocation:
            mark_product_as_used(s1_uuid='None', s1_date='None', s2_uuid=s2_id, s2_date=s2_date, collocated_folder = dir_out_for_roi)
        return filename_s2_collocated

    def crop(self, dir_out_for_roi, s1_or_s2, product_id, path_collocated, ROI_subset, roi_no):
        s1_or_s2 = s1_or_s2.upper()
        assert s1_or_s2 in ["S1", "S2"], "s1_or_s2 must be 'S1' or 'S2'"
        roi_path = str(dir_out_for_roi / f"ROI{roi_no}.geojson")
        raster = rio.open(path_collocated)

#         print('ROI_subset',ROI_subset)
        # Don't use 'init' keyword, as it's deprecated
        wgs84 = pyproj.Proj(init="epsg:4326")
        utm = pyproj.Proj(init=str(raster.crs))
        project = partial(pyproj.transform, wgs84, utm)
        utm_ROI = transform(project, ROI_subset)
#         utm_ROI = utm_ROI.intersection(
#             utm_ROI
#         )  ##Just a way around make multipolygon to polygon
        if not hasattr(utm_ROI, 'exterior'):
            logging.warning("utm_ROI doesn't have an 'exterior'")
            logging.warning(f"Type of utm_ROI: {str(type(utm_ROI))}")
        try:
            ### For polygons exterior.coords exists
            utm_ROI = Polygon(list((utm_ROI.exterior.coords)))
            utm_ROI_m = MultiPolygon([utm_ROI])
#             print('Polygon utm_ROI', utm_ROI, 'str(raster.crs)', str(raster.crs))
        except Exception as E:
#             if DEBUG:
#                 pdb.set_trace()
#             else:
#                 raise E
            ### For multi polygons exterior.coords does not exist, so converting it to polygon
            utm_ROI = multipolygon_to_polygon(utm_ROI)
            if utm_ROI.is_valid == False:
                utm_ROI = utm_ROI.buffer(0)
            utm_ROI_m = MultiPolygon([utm_ROI])
#             print('Multipolygon utm_ROI', utm_ROI, 'str(raster.crs)', str(raster.crs))
        ROI_gpd=gpd.GeoDataFrame(utm_ROI_m, crs=str(raster.crs))
        ROI_gpd = ROI_gpd.rename(columns={0: 'geometry'})
        ROI_gpd.set_geometry(col='geometry', inplace=True) ## explicitly set it as geometry for the GeoDataFrame
        ROI_gpd.to_file(roi_path, driver='GeoJSON')

        dir_out_clipped = dir_out_for_roi / s1_or_s2 / "Clipped"

        # # Make directory for the clipped file,
        # # and don't complain if it already exists
        dir_out_clipped.mkdir(exist_ok=True, parents=True)

        filename = "{}_roi{}_{}.tif".format(s1_or_s2, roi_no, product_id)
        clipped_file_path = dir_out_clipped / filename
        if clipped_file_path.exists():
            clipped_file_path.unlink()  # Delete a clipped file if it exists

        ### S1 collocated has datatype of float32 while S2 collocated has datatype of Uint16, but if we dont pass output type to gdal warp, it saves both S1 and S2 clipped file in float32 and so for S2, clipped file has much larger size than collocated file
        ### To check later
#         if s1_or_s2 == 'S1':
#             gdal_result = gdal.Warp(str(clipped_file_path),str(path_collocated),cutlineDSName = str(roi_path), cropToCutline=True, dstNodata=999999999.0)
#         elif s1_or_s2 == 'S2':
#             gdal_result = gdal.Warp(str(clipped_file_path),str(path_collocated),cutlineDSName = str(roi_path), cropToCutline=True, dstNodata=999999999.0, outputType=gdal.gdalconst.GDT_UInt16)
        gdal_result = gdal.Warp(str(clipped_file_path),str(path_collocated),cutlineDSName = str(roi_path), cropToCutline=True, dstNodata=999999999.0,  warpOptions=['CUTLINE_ALL_TOUCHED=TRUE'])
        gdal_result = None ## Important to initial gdal writing operations

        raster.close()

        return clipped_file_path


    def make_patches(self, dir_out, clip_path, s1_or_s2, s1_id, s2_id):
        """Make smaller (potentially overlapping) patches from a geotiff.

        Arguments
        ---------
        dir_out : pathlib.Path
            Directory to save patches
        clip_path : pathlib.Path
            Filename of cropped sentinel geotiff image
        s1_or_s2 : str
            Either "S1" or "S2"
        s1_id : str
            UUID of the SEN1 product
        s2_id : str
            UUID of the SEN2 product

        Returns
        -------
        NO RETURN
        """

        print('Making ',s1_or_s2,' patches')
        # Convert from pathlib.Path to str
        s1_or_s2 = s1_or_s2.upper()
        assert s1_or_s2 in ["S1", "S2"], "s1_or_s2 must be 'S1' or 'S2'"

        clip_path = str(clip_path)
        raster = rio.open(clip_path)
        raster_im = raster.read(masked=False)
        res = int(raster.res[0])  # Assuming the resolution in both direction as equal
        gdal_dataset = gdal.Open(clip_path)

        # Create a directory to store the patches
        dir_out.mkdir(exist_ok=True, parents=True)
        step_row, step_col = 1 - self.overlap[0], 1 - self.overlap[1]
        row_stride = int(self.size[0] * step_row)
        col_stride = int(self.size[1] * step_col)
        for row_pixel_start in range(0, raster_im.shape[1] - self.size[0]+1, row_stride):
            for column_pixel_start in range(
                0, raster_im.shape[2] - self.size[1]+1, col_stride
            ):
                row_pixel_end = row_pixel_start + self.size[0] - 1
                column_pixel_end = column_pixel_start + self.size[1] - 1
                # Size is (height, width), as per Priti's code,
                # so display size[1]_size[0] (`width_height`) in filename

                if (self.skip_secondary == True) and (self.primary =='S1'):
                    patch_filename = (
                        f"S1_{s1_id}"
                        + f"_{row_pixel_start}_{column_pixel_start}"
                        + f"_{self.size[1]}x{self.size[0]}.tif"
                    )

                elif (self.skip_secondary == True) and (self.primary =='S2'):
                    patch_filename = (
                        f"S2_{s2_id}"
                        + f"_{row_pixel_start}_{column_pixel_start}"
                        + f"_{self.size[1]}x{self.size[0]}.tif"
                    )

                else:
                    patch_filename = (
                        f"S1_{s1_id}"
                        + f"_S2_{s2_id}"
                        + f"_{row_pixel_start}_{column_pixel_start}"
                        + f"_{self.size[1]}x{self.size[0]}.tif"
                    )

                output_filename = str(dir_out / patch_filename)

                start_x, start_y = raster.xy(row_pixel_start, column_pixel_start)
                start_x = start_x - res / 2
                start_y = start_y + res / 2

                end_x, end_y = raster.xy(row_pixel_end, column_pixel_end)
                end_x = end_x + res / 2
                end_y = end_y - res / 2

                projwin = [start_x, start_y, end_x, end_y]
                gdal.Translate(
                    output_filename, gdal_dataset, format="GTiff", projWin=projwin
                )
        raster.close()

    def download(self):
        """Download available products."""
        download_start_time = datetime.now()
        print("Downloading started")

        if not self.ran_list:
            print("Haven't searched products yet. Running `.find_products()`")
            self.find_products()

        # If we aren't loading the geojson from the region's dir, copy it there
        # if not 'SENTINEL_STORAGE_PATH' in os.environ:
        #     msg = "SENTINEL_STORAGE_PATH must be set using environment variable."
        #     msg += "\nCan also set using os.environ['SENTINEL_STORAGE_PATH'] = '...path...'."
        #     raise Exception(msg)
        # SENTINEL_STORAGE_PATH = os.environ['SENTINEL_STORAGE_PATH']
        parent_dir = Path(SENTINEL_STORAGE_PATH) / self.roi_name
        parent_dir.mkdir(exist_ok=True, parents=True)

        for _,s1, s2, s1_old, ROI_subset,_, roi_no in self.product_map:
            if s1 is not None:  # Download Sentinel 1 Product
                if not self.external_bucket:
                    if self.api.get_product_odata(s1["uuid"])['Online']== True:
                        print('\nDownloading', s1["title"],'from sentinelsat')
                        self.api.download(s1["uuid"], directory_path=SENTINEL_ROOT, checksum=True)
                    else:
                        download_S1_NOAA(s1)
                        print('\nDownloading', s1["title"],'from NOAA as it is not online on copernicus')
                else:
                    print('\nDownloading', s1["title"],'from NOAA')
                    download_S1_NOAA(s1)

            if s2 is not None:  # Download Sentinel 2 Product
                if not self.external_bucket:
                    if self.api.get_product_odata(s2["uuid"])['Online']== True:
                        print('\nDownloading', s2["title"],'from sentinelsat')
                        self.api.download(s2["uuid"], directory_path=SENTINEL_ROOT, checksum=True)
                    else:
                        print('\nDownloading', s2["title"],'from Google as it is not online on copernicus')
                        download_S2_GCS(s2)

                else:
#                         print('\nDownloading', s2["uuid"],'from sentinelhub')
#                         download_S2_sentinelhub(s2)
                   print('\nDownloading', s2["title"],'from Google')
                   download_S2_GCS(s2)
            if s1_old is not None:  # Download Sentinel 1 Product
                if not self.external_bucket:
                    if self.api.get_product_odata(s1_old["uuid"])['Online']== True:
                        print('\nDownloading', s1_old["title"],'from sentinelsat')
                        self.api.download(s1_old["uuid"], directory_path=SENTINEL_ROOT, checksum=True)
                    else:
                        download_S1_NOAA(s1_old)
                        print('\nDownloading', s1_old["title"],'from NOAA as it is not online on copernicus')
                else:
                    print('\nDownloading', s1_old["title"],'from NOAA')
                    download_S1_NOAA(s1_old)
        download_time_taken = datetime.now() - download_start_time
        print("Time taken for downloading is ", download_time_taken)

    def process(self):
        """Download and preprocess available products."""
        process_start_time = datetime.now()
        logging.info("Preprocessing started")

        if not self.ran_list:
            print("Haven't searched products yet. Running `.find_products()`")
            self.find_products()
        print('\n \n-----------------------------------------------------------------------')
        print('-----------------------------------------------------------------------')
        print('Products processing started')

        # If we aren't loading the geojson from the region's dir, copy it there
        # if not 'SENTINEL_STORAGE_PATH' in os.environ:
        #     msg = "SENTINEL_STORAGE_PATH must be set using environment variable."
        #     msg += "\nCan also set using os.environ['SENTINEL_STORAGE_PATH'] = '...path...'."
        #     raise Exception(msg)
        # SENTINEL_STORAGE_PATH = os.environ['SENTINEL_STORAGE_PATH']
        parent_dir = Path(SENTINEL_STORAGE_PATH) / self.roi_name
        parent_dir.mkdir(exist_ok=True, parents=True)
        # Previously, copied the geojson into the region's output folder
        # but using CT's suggestion, may be possible/wise to remove this code
        # filename_geojson = self.roi["filename"]
        # if not Path(filename_geojson).parent == parent_dir:
        #     shutil.copy(filename_geojson, parent_dir)


        if self.rebuild:
            logging.info("Rebuilding products")
            s1_s2_products_existing = pd.DataFrame()
        else:
            s1_s2_products_existing = existing_processed_products()

        for start,s1,  s2, s1_old, ROI_subset,_, roi_no in self.product_map:

            if self.primary == 'S2':
                dir_out_for_roi = (
                    Path(SENTINEL_STORAGE_PATH)
                    / self.roi_name
                    /  start.strftime("%Y%m%d")
                    / f"ROI{roi_no}"
                    )
            else:
                dir_out_for_roi = (
                    Path(SENTINEL_STORAGE_PATH)
                    / self.roi_name
                    / start.strftime("%Y%m%d")
                    / f"ROI{roi_no}"
                    )
            print("ROI Dir", dir_out_for_roi)

            if s1 is not None:
                s1_id = s1["uuid"]
                logging.info(f"- S1 {s1_id}")
                s1_date = s1.beginposition.strftime("%Y%m%d")
                s1_title = s1["title"]
                dir_out_S1_patches = dir_out_for_roi / "S1" / "Patches"
                print('s1_title',s1_title)

            if s2 is not None:
                s2_id = s2.uuid
                logging.info(f"Processing ROI subset {ROI_subset}, S2 {s2_id}")
                s2_date = s2.beginposition.strftime("%Y%m%d")
                # if has_product_been_used(s2_id):
                #     print(f"Skipping used S2 product {s2_id}")
                #     continue
                s2_title = s2.title
                dir_out_S2_patches = dir_out_for_roi / "S2" / "Patches"
                print('s2_title',s2_title)

            if s1_old is not None:
                s1_old_id = s1_old["uuid"]
                logging.info(f"- S1 old {s1_old_id}")
                s1_old_date = s1_old["beginposition"].strftime("%Y%m%d")
                s1_old_title = s1_old["title"]
                print('s1_old_title',s1_old_title)

            products_exist = False
            if not self.rebuild:
                path_s1_collocated, path_s2_collocated  = None, None

                if (s1 is not None) and (s2 is not None):
                     ## Check whether the collocated products exist
                    if isinstance(s1_s2_products_existing, pd.DataFrame):
                        existing_products=s1_s2_products_existing[(s1_s2_products_existing['S1-uuid']==s1_id) & (s1_s2_products_existing['S2-uuid']==s2_id)]
                        if  len(existing_products)>0:
                            ## sort the products to use the latest collocated product
                            existing_products= existing_products.sort_values(by=["Processed-date"], ascending=(False))
                            selected_used_product =  existing_products.iloc[0]
                            existing_collocated_path = selected_used_product['Collocated-folder']

                            imagename = f"S1_{s1_id}_S2_{s2_id}.tif"
                            path_s1_collocated = Path(existing_collocated_path) / "S1" / "Collocated" / imagename
                            path_s2_collocated = Path(existing_collocated_path) / "S2" / "Collocated" / imagename

                            if path_s1_collocated.exists() and path_s2_collocated.exists():
                                products_exist = True
                                print('S1 and S2 products exist in earlier used-products, So collocation is skipped')
                                filename_s1_collocated = dir_out_for_roi / "S1" / "Collocated" / imagename
                                filename_s2_collocated = dir_out_for_roi / "S2" / "Collocated" / imagename

                                filename_s1_collocated.parent.mkdir(exist_ok=True, parents=True)
                                filename_s2_collocated.parent.mkdir(exist_ok=True, parents=True)
                elif (s1 is not None):
                    ## Check whether the processed S1 products exist
                    if isinstance(s1_s2_products_existing, pd.DataFrame):
                        existing_products=s1_s2_products_existing[(s1_s2_products_existing['S1-uuid']==s1_id)]
                        if  len(existing_products)>0:
                            ## sort the products to use the latest collocated product
                            existing_products= existing_products.sort_values(by=["Processed-date"], ascending=(False))
                            selected_used_product =  existing_products.iloc[0]
                            existing_collocated_path = selected_used_product['Collocated-folder']
                            if (selected_used_product['S2-date']=='None'):
                                imagename = f"S1_{s1_id}.tif"
                            else:
                                s2_id_prev = selected_used_product['S2-uuid']
                                imagename = f"S1_{s1_id}_S2_{s2_id_prev}.tif"
                            path_s1_collocated = Path(existing_collocated_path) / "S1" / "Collocated" / imagename
                            if path_s1_collocated.exists():
                                products_exist = True
                                print('S1 product exists in earlier used-products, So snap-processing is skipped')
                                filename_s1_collocated = dir_out_for_roi / "S1" / "Collocated" / imagename
                                filename_s1_collocated.parent.mkdir(exist_ok=True, parents=True)

                elif (s2 is not None):
                    if isinstance(s1_s2_products_existing, pd.DataFrame):
                        existing_products=s1_s2_products_existing[(s1_s2_products_existing['S2-uuid']==s2_id)]
                        if  len(existing_products)>0:
                            ## sort the products to use the latest collocated product
                            existing_products= existing_products.sort_values(by=["Processed-date"], ascending=(False))
                            selected_used_product =  existing_products.iloc[0]
                            existing_collocated_path = selected_used_product['Collocated-folder']
                            if selected_used_product['S1-date']=='None':
                                imagename = f"S2_{s2_id}.tif"
                            else:
                                s1_id_prev = selected_used_product['S1-uuid']
                                imagename = f"S1_{s1_id_prev}_S2_{s2_id}.tif"
                            path_s2_collocated = Path(existing_collocated_path) / "S2" / "Collocated" / imagename
                            if  path_s2_collocated.exists():
                                products_exist = True
                                print('S2 product exists in earlier used-products, So snap-processing is skipped')
                                filename_s2_collocated = dir_out_for_roi / "S2" / "Collocated" / imagename
                                filename_s2_collocated.parent.mkdir(exist_ok=True, parents=True)

            if ((self.rebuild) or ((not self.rebuild) and (not products_exist))):
                try:
                    if (s1 is not None) and (s2 is not None):

                        if self.multitemporal and not self.S1_SLC:
                            path_s1_collocated, path_s2_collocated = self.collocate_GRD_multitemporal(dir_out_for_roi,ROI_subset, s1_title, s1_id, s1_date, s2_title, s2_id, s2_date, s1_old_title, s1_old_id, s1_old_date)

                        elif self.multitemporal and self.S1_SLC:
                            path_s1_collocated, path_s2_collocated = self.collocate_SLC_multitemporal(dir_out_for_roi,ROI_subset, s1_title, s1_id, s1_date, s2_title, s2_id, s2_date, s1_old_title, s1_old_id, s1_old_date)
                        else:
                            path_s1_collocated, path_s2_collocated = self.collocate(
                            dir_out_for_roi, ROI_subset, s1_title, s1_id, s1_date, s2_title, s2_id, s2_date)

                    elif (s1 is not None):
                        path_s1_collocated= self.snap_s1(
                            dir_out_for_roi, ROI_subset, s1_title, s1_id, s1_date)

                    elif (s2 is not None):
                        path_s2_collocated = self.snap_s2(
                            dir_out_for_roi, ROI_subset, s2_title, s2_id, s2_date)

                except CoordinateOutOfBoundsError as E:
                        # log known bug
                        logging.error(E)
                        continue
                except Exception as E:
                        # log unknown bug
                        logging.error(E)
                        raise E
            # Crop sentinel-1 products
            if (s1 is not None):
                if not path_s1_collocated:
                    logging.error(f"No S1 collocation file for {s1_id}, so either no products, or issue with S1 products")
                    continue

                s1_clip_path = self.crop(
                    dir_out_for_roi,
                    "S1",
                    s1_id,
                    path_s1_collocated,
                    ROI_subset,
                    roi_no,
                )
                if self.skip_secondary:
                    s2_id =None
                try:
                    self.make_patches(dir_out_S1_patches, s1_clip_path, "S1", s1_id, s2_id)
                except rio.errors.RasterioIOError:
                    pass

            # Crop sentinel-2 products
            if (s2 is not None):
                if not path_s2_collocated:
                    logging.error(f"No S2 collocation file for {s2_id}, so either no products, or issue with S2 products")
                    continue

                s2_clip_path = self.crop(
                                        dir_out_for_roi,
                                        "S2",
                                        s2_id,
                                        path_s2_collocated,
                                        ROI_subset,
                                        roi_no,
                                               )
                if self.skip_secondary:
                    s1_id =None
                try:
                    self.make_patches(dir_out_S2_patches, s2_clip_path, "S2", s1_id, s2_id)
                except rio.errors.RasterioIOError:
                    pass

        process_time_taken = datetime.now() - process_start_time
        print('Time taken for processing', process_time_taken)
        logging.info("Preprocessing finished")


    def run(self):
        """Run the pipeline."""
        self.find_products()
        self.display_available()
        if self.mode in ["download", "download_process", "all"]:
            self.download()
        if self.mode in ["process", "download_process", "all"]:
            self.process()




# TODO -- Priti, please don't move the default command line stuff around ('main') in order
# to add personal customisation like the 'docopt_arg_extension'
# I would recommend creating a personal script if you wish to use a different format.
# If the CLI gets new flags (like S1_SLC), then add them to the main, but the docopt_arg_extension
# makes this code more difficult to maintain
def main():
    """Provide a CLI interface for sentinel processing or config creation."""
    from docopt import docopt

    args = docopt(__doc__)

    log_fmt = "%(levelname)s : %(asctime)s : %(message)s"
    log_level = logging.DEBUG
    log_also_to_stderr = False

    config_basename = Path(args["--config"]).name
    # log_filename = f'logs/{config_basename}.log'
    # logging.basicConfig(level=log_level, format=log_fmt, filename=log_filename)
    if log_also_to_stderr:
        logging.getLogger().addHandler(logging.StreamHandler())

    # Use 'mode' and the SentinelProcessor.run() function
    # to prevent duplicate code and make it easier to modify and add more functionality
    # we _always_ do prepper.find_products() and prepper.download()
    # (this happens inside prepper.run())
    if args["create"]:
        # If we want to create a config, just return early
        configutil.create(args["-c"])
        return
    # else...choose what the sentinel preprocessor should do
    elif args["list"]:
        mode = "list"
    elif args["download"]:
        mode = "download"
    elif args["process"]:
        mode = "process"
    elif args["download_process"]:
        mode = "download_process"
    else:
        # Using docopt, this shouldn't be accessible
        # If the appropriate args aren't used, docopt will auto display help
        logging.warning(
            f"Shouldn't be able to reach this branch, due to docopt: args {args}"
        )

    prepper = SentinelPreprocessor(
        config_filename=args['--config'],
        credentials=args['--credentials'],
        rebuild=args['--rebuild'],
        full_collocation=args['--full_collocation'],
        skip_week=args['--skip_week'],
        primary=args['--primary'],
        skip_secondary=args['--skip_secondary'],
        external_bucket=args['--external_bucket'],
        available_area=args['--available_area'],
        multitemporal=args['--multitemporal'],
        S1_SLC=args['--S1_SLC'],
        secondary_time_delta=args['--secondary_time_delta'],
        primary_prod_frequency=args['--primary_prod_frequency'],
        cloud_mask_filtering=args['--cloud_mask_filtering'],
        mode=mode,
    )
    prepper.run()


if __name__ == "__main__":
    main()
