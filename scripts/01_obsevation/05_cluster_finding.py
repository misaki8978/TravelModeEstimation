#! /usr/bin/env python3
import os
import warnings
import pandas as pd
import sys
import gzip
import osmnx as ox
from japanmap import get_data, pref_points, pref_names
from shapely.geometry import Polygon
import geopandas as gpd

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from clustering_func import cluster_pairplot, fuzzy_cluster_3Dplot, cluster_boxplot, map_plot
from file_open import division_two_file
from gis import train_from_OSM

warnings.filterwarnings('ignore')

files = sys.argv[1:]  # 引数でファイルリストを受け取る
gps_file, gis_file = division_two_file(files)

message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_05_cluster_finding.txt"


YEAR = gps_file.split("/")[-2]
PLACE = gps_file.split("/")[-3].split("_")[-2:]
PLACE = "_".join(PLACE)
OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/05_cluster_finding/{PLACE}/{YEAR}"
os.makedirs(OUT_DIR, exist_ok=True)

with gzip.open(gps_file, 'rt') as f:
    df = pd.read_csv(f)
    f.close()
log_message(f"df.columns: {df.columns}", message_path)
feature_cols = [
        'mean_vel',
        'max_vel',
        'min_vel',
        'mean_acc',
        'total_distance',
        'duration_sec',
        'bearing_rate_rad'
    ]
# cluster_pairplot(df, feature_cols, OUT_DIR)
# df = fuzzy_cluster_3Dplot(df, OUT_DIR)
# df_07 = df.query("max_membership >= 0.9")
# log_message(f"df_07.head: {df_07.head()}", message_path)
# cluster_boxplot(df_07, feature_cols, OUT_DIR)

#都道府県ポリゴン
pref_poly = [Polygon(points) for points in pref_points(get_data())]
gdf_pref = gpd.GeoDataFrame(crs = 'EPSG:4326', geometry=pref_poly)
gdf_pref['prefecture'] = pref_names[1:]
gdf_pref = gdf_pref.to_crs(epsg=32652)

#GPSデータをGeoDataFrameに変換
gps_gdf = gpd.GeoDataFrame(
    df,
    geometry=gpd.points_from_xy(df["longitude_anonymous"], df["latitude_anonymous"]),
    crs="EPSG:4326",
)
gps_gdf = gps_gdf.to_crs(epsg=32652)

#GIS--bus
bus_df = gpd.read_file(gis_file)
bus_routes = bus_df[bus_df.geometry.geom_type.isin(["LineString", "MultiLineString"])]
bus_gdf = bus_routes.to_crs(epsg=32652)

place_name = "Nagasaki, Japan"
#GIS--rail
train_gdf = train_from_OSM(place_name)

map_plot(gps_gdf, OUT_DIR, gdf_pref, bus_gdf, train_gdf, "normal")
map_plot(gps_gdf, OUT_DIR, gdf_pref, bus_gdf, train_gdf, "gis")









