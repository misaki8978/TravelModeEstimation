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
from gis import  plot_gpspoint

log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/log_09_plot_all.txt"

files = sys.argv[1:]
# gps_file = files[0]
bus_routes = files[-4]
rail_routes = files[-3]
stop_bus = files[-2]
stop_rail = files[-1]

# part_file = gps_file.split('/')
# place = part_file[-3]
# year = part_file[-2]
place = "07_osaka"
year = "2019_weekly"
out_dir = f"/home/data/fukui/outputs/figures/{place}/{year}/09_04_gis"
os.makedirs(out_dir, exist_ok=True)

place_name = "Nagasaki, Japan"

pref_poly = [Polygon(points) for points in pref_points(get_data())]
gdf_pref = gpd.GeoDataFrame(crs = 'EPSG:4326', geometry=pref_poly)
gdf_pref['prefecture'] = pref_names[1:]
gdf_pref = gdf_pref.to_crs(epsg=4326)

# df = pd.read_csv(gps_file, compression='gzip')
# gps_gdf = gpd.GeoDataFrame(
#         df,
#         geometry=gpd.points_from_xy(df["longitude_anonymous"], df["latitude_anonymous"]),
#         crs="EPSG:4326",
#     )
bus_routes = gpd.read_file(bus_routes, encoding='cp932', config_options={"SHAPE_RESTORE_SHX": "YES"})
rail_routes = gpd.read_file(rail_routes, encoding='cp932', config_options={"SHAPE_RESTORE_SHX": "YES"})
stop_bus = gpd.read_file(stop_bus, encoding='cp932', config_options={"SHAPE_RESTORE_SHX": "YES"})
stop_rail = gpd.read_file(stop_rail, encoding='cp932', config_options={"SHAPE_RESTORE_SHX": "YES"})

bus_routes = bus_routes.set_crs(epsg=4326)
rail_routes = rail_routes.set_crs(epsg=4326)
stop_bus = stop_bus.set_crs(epsg=4326)
stop_rail = stop_rail.set_crs(epsg=4326)

fig, ax = plt.subplots(figsize=(12, 12))

gdf_pref.query("prefecture == '大阪府'", engine='python').plot(ax=ax, color='lightgray')
rail_routes.plot(ax=ax, color='red', linewidth=1, label='Rail', alpha=0.7)
stop_rail.plot(ax=ax, color='red', markersize=3, marker='*', label='Stations', alpha=0.7)
bus_routes.plot(ax=ax, color="blue", linewidth=1, label="Bus Routes", alpha=0.3)
stop_bus.plot(ax=ax, color="blue", markersize=3, marker='o', label="Bus Stops", alpha=0.3)

ax.set_xlim(135.0, 135.8)
ax.set_ylim(34.2, 35.1) #大阪
# ax.set_xlim(129.5, 130.5)
# ax.set_ylim(32.5, 33.5) #長崎

ax.axis('off')
plt.legend()
fig.savefig(f"{out_dir}/all_gis_layer.png")






color_col = {"bus": "blue", "rail": "red", "bus_layer": "skyblue", "rail_layer": "pink"}

# plot_gis_layer(gps_gdf, bus_routes, gdf_pref, "bus", out_dir, color_col)
# plot_gis_layer(gps_gdf, train_gdf, gdf_pref, "rail", out_dir, color_col)
# plot_gpspoint(df, gdf_pref.query("prefecture == '長崎県'", engine='python'), out_dir)
