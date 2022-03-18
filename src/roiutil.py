import json
import tempfile
from functools import partial

import geopandas as gpd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pyproj
from descartes.patch import PolygonPatch
from sentinelsat import read_geojson, geojson_to_wkt
from shapely.geometry import Polygon, MultiPolygon, shape
from shapely.ops import transform
from shapely.wkt import dumps as wktdump
from shapely.wkt import loads as wktload
import numpy as np


class ROI:
    def __init__(self, geojson):
        """Create a ROI object.

        Will load footprint, shapely geometry, and features into a class.

        Arguments
        ---------
        geojson : str
            String of geojson data

        Returns
        -------
        ROI instance
        """
        self.footprint = None
        with tempfile.NamedTemporaryFile("w") as f:
            f.write(geojson)
            f.seek(0)
            self.footprint = geojson_to_wkt(read_geojson(f.name))
        # self.cloud_cover = config['cloud_cover']
        # This may be a bit brittle to keep the geojson within the config file
        self.features = json.loads(geojson)["features"][0]
        self.shape = shape(self.features["geometry"])


    def plot(self, grid=False):
        """Plots region of Interest

        Arguments
        ---------
        grid : bool
            Whether to plot with a grid

        Returns
        -------
        ax : matplotlib.axes
            Axis of the plot
        """
        fig = plt.figure(figsize=(5, 5))
        ax = fig.add_subplot(111)
        ax.set_title("Region of Interest")
        ax.add_patch(PolygonPatch(self.shape, fc="yellow"))

        if grid == True:
            ax.grid(True)
        ax.axis("equal")
        return ax

    @staticmethod
    def load_from_file(filename):
        """Loads ROI in a shapely geometry

        Arguments
        ---------
        filename : str or os.PathLike
            Path to the geojson file

        Returns
        -------
        shapely geometry
        """
        with open(filename) as f:
            features = json.load(f)["features"]

        return ROI(features)

    def to_multipolygon(self):
        return MultiPolygon([wktload(wktdump(self.shape))])


def export_to_file(roi, filename, crs):
    wgs84 = pyproj.Proj(init="epsg:4326")
    utm = pyproj.Proj(init=str(crs))
    project = partial(pyproj.transform, wgs84, utm)
    utm_ROI = transform(project, roi)

    if not hasattr(utm_ROI, "exterior"):
        print("utm_ROI doesn't have an 'exterior'")
        print(f"Type of utm_ROI: {str(type(utm_ROI))}")
    try:
        ### For polygons exterior.coords exists
        utm_ROI = Polygon(list((utm_ROI.exterior.coords)))
        utm_ROI_m = MultiPolygon([utm_ROI])
    except Exception as E:
        ### For multi polygons exterior.coords does not exist

        area_list = [x.area for x in utm_ROI]
        area_array = np.array(area_list)
        max_area_polygon_no = np.argmax(area_array)
        utm_ROI = utm_ROI[max_area_polygon_no]
        if utm_ROI.is_valid == False:
            utm_ROI = utm_ROI.buffer(0)
        utm_ROI_m = MultiPolygon([utm_ROI])

    ROI_gpd = gpd.GeoDataFrame(utm_ROI_m, crs=str(crs))
    ROI_gpd = ROI_gpd.rename(columns={0: "geometry"})
    # explicitly set it as geometry for the GeoDataFrame
    ROI_gpd.set_geometry(col="geometry", inplace=True)
    ROI_gpd.to_file(filename, driver="GeoJSON")
