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
from segment_analysis_func import mode_change, plot_ratio_by_user

warnings.filterwarnings('ignore')

gps_file = sys.argv[1]  # 引数でファイルリストを受け取る
# gps_file, gis_file = division_two_file(files)
gis_file = sys.argv[-2]
train_file = sys.argv[-1]

message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_05_cluster_finding.txt"

train_cluster_col = {
   "-1": "walk", 
   "0": "train", 
   "1": "bus", 
   "2": "two-wheeler", 
   "3": "car",
   "4": "bike"
   }
bus_cluster_col = {
   "-1": "walk", 
   "0": "car", 
   "1": "two-wheeler", 
   "2": "bus",
   "3": "train"
   }
other_cluster_col = {
   "-1": "walk", 
   "0": "two-wheeler", 
   "1": "car",
   "2": "bike"
   }


YEAR = gps_file.split("/")[-2]
PLACE = gps_file.split("/")[-3]
# log_message(f"PLACE: {PLACE}", message_path)
OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/{PLACE}/{YEAR}/05_cluster_find"
os.makedirs(OUT_DIR, exist_ok=True)

#都道府県ポリゴン
pref_poly = [Polygon(points) for points in pref_points(get_data())]
gdf_pref = gpd.GeoDataFrame(crs = 'EPSG:4326', geometry=pref_poly)
gdf_pref['prefecture'] = pref_names[1:]
gdf_pref = gdf_pref.to_crs(epsg=32652)
# gps_list = []
# for gps_file in gps_files:
#     gis_name = gps_file.split("/")[-1].split("_")[0]
#     with gzip.open(gps_file, 'rt') as f:
#         df = pd.read_csv(f)
#         df = mode_change(df, eval(f"{gis_name}_cluster_col"), gis_name)
#         # log_message(f"{len(df)}points", message_path)
#         gps_list.append(df)
#         f.close()
    
    # log_message(f"{gis_name}: {df[f'mode_label'].value_counts()}", message_path)
    # feature_cols = [
    #         'mean_vel',
    #         'max_vel',
    #         'min_vel',
    #         'mean_acc',
    #         'total_distance',
    #         'duration_sec',
    #         'bearing_rate_rad'
        # ]
# cluster_pairplot(df, feature_cols, OUT_DIR)
# df = fuzzy_cluster_3Dplot(df, OUT_DIR)
# df_07 = df.query("max_membership >= 0.9")
# log_message(f"df_07.head: {df_07.head()}", message_path)
# cluster_boxplot(df_07, feature_cols, OUT_DIR)

# gps_df = pd.concat(gps_list)
# log_message(f"{gps_df['mode_label'].value_counts()}", message_path)
gps_df = pd.read_csv(gps_file, compression="gzip")
log_message(f"gps_df: {gps_df.head()}", message_path)
#GPSデータをGeoDataFrameに変換
gps_gdf = gpd.GeoDataFrame(
    gps_df,
    geometry=gpd.points_from_xy(gps_df["longitude_anonymous"], gps_df["latitude_anonymous"]),
    crs="EPSG:4326",
)
gps_gdf = gps_gdf.to_crs(epsg=32652)

#GIS--bus
bus_df = gpd.read_file(gis_file, encoding='cp932', config_options={"SHAPE_RESTORE_SHX": "YES"})

bus_gdf = bus_df.set_crs(epsg=4326)
bus_gdf = bus_gdf.to_crs(epsg=32652)

train_df = gpd.read_file(train_file, encoding='cp932', config_options={"SHAPE_RESTORE_SHX": "YES"})

train_gdf = train_df.set_crs(epsg=4326)
train_gdf = train_gdf.to_crs(epsg=32652)

log_message(f"OUT_DIR: {OUT_DIR}", message_path)
map_plot(gps_gdf, OUT_DIR, gdf_pref, bus_gdf, train_gdf)
log_message("done", message_path)
plot_ratio_by_user(gps_df, OUT_DIR)









