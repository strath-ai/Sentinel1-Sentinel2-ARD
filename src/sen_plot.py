from descartes.patch import PolygonPatch
import matplotlib.pyplot as plt

def plot_ROI(ROI, grid=False):
    """
    Plots region of Interest
    Parameters:
    s_product_df: geopandas.geodataframe.GeoDataFrame returned from Sentinelsat api
    ROI: shapely.geometry.multipolygon.MultiPolygon
    """
    fig = plt.figure(figsize=(5, 5))
    ax = fig.add_subplot(111)
    ax.set_title("Region of Interest")
    ax.add_patch(PolygonPatch(ROI, fc="yellow"))

    if grid == True:
        ax.grid(True)
    ax.axis("equal")


def plot_Stiles_plus_ROI(ROI, s_products_df, s_tiles_color="green", grid=False, title ="Sentinel tiles and ROI"):

    """
    Plots Sentinel tiles along with ROI
    Parameters:
    s_product_df: geopandas.geodataframe.GeoDataFrame returned from Sentinelsat api
    ROI: shapely.geometry.multipolygon.MultiPolygon

    """
    # S1 or S2 tiles

    fig = plt.figure(figsize=(5, 5))
    ax = fig.add_subplot(111)

    for i in range(0, s_products_df.shape[0]):
        geometry = s_products_df.iloc[i]["geometry"]
        ax.add_patch(PolygonPatch(geometry, fc=s_tiles_color))
        ax.set_title(title)

    if grid == True:
        ax.grid(True)

    ax.add_patch(PolygonPatch(ROI, fc="yellow"))
    ax.axis("equal")


def plot_S1S2tiles_plus_ROI(ROI, s1_products_df, s2_products_df, grid=False, title ="Sentinel-1,2 tiles and ROI"):
    """
    Plots Sentinel-1 and Sentinel-2 tiles along with ROI
    Parameters:
    s1_product_df: geopandas.geodataframe.GeoDataFrame returned from Sentinelsat api
    s2_product_df: geopandas.geodataframe.GeoDataFrame returned from Sentinelsat api
    ROI: shapely.geometry.multipolygon.MultiPolygon

    """
    # S1 or S2 tiles

    fig = plt.figure(figsize=(5, 5))
    ax = fig.add_subplot(111)

    for i in range(0, s1_products_df.shape[0]):
        geometry = s1_products_df.iloc[i]["geometry"]
        ax.add_patch(PolygonPatch(geometry, fc="blue"))

    for j in range(0, s2_products_df.shape[0]):
        geometry = s2_products_df.iloc[j]["geometry"]
        ax.add_patch(PolygonPatch(geometry, fc="green"))
        ax.set_title(title)

    if grid == True:
        ax.grid(True)

    ax.add_patch(PolygonPatch(ROI, fc="yellow"))
    ax.axis("equal")

