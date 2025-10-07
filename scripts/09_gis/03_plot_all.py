import os
import sys
import gzip
import numpy as np
import pandas as pd
from glob import glob
import geopandas as gpd
import plotly.graph_objects as go
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
import matplotlib.pyplot as plt
import osmnx as ox
from japanmap import get_data, pref_points, pref_names

import warnings
warnings.filterwarnings('ignore')

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from gis import plot_gis_layer, division_2file, train_from_OSM, plot_gpspoint

log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/log_09_plot_all.txt"

files = sys.argv[1:]
bus_routes, gps_gdf, year, place = division_2file(files)

out_dir = f"/home/data/fukui/outputs/figures/09_nagasaki/{year}/09_04_gis"
os.makedirs(out_dir, exist_ok=True)

place_name = "Nagasaki, Japan"

pref_poly = [Polygon(points) for points in pref_points(get_data())]
gdf_pref = gpd.GeoDataFrame(crs = 'EPSG:4326', geometry=pref_poly)
gdf_pref['prefecture'] = pref_names[1:]
gdf_pref = gdf_pref.to_crs(epsg=32652)

train_gdf = train_from_OSM(place_name)


color_col = {"bus": "blue", "rail": "red", "bus_layer": "skyblue", "rail_layer": "pink"}

plot_gis_layer(gps_gdf, bus_routes, gdf_pref, "bus", out_dir, color_col)
plot_gis_layer(gps_gdf, train_gdf, gdf_pref, "rail", out_dir, color_col)
plot_gpspoint(gps_gdf, bus_routes, gdf_pref, out_dir, color_col)
