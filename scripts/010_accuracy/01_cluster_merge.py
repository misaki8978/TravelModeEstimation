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
from clustering_func import map_plot
from accuracy_cal import mode_change

warnings.filterwarnings('ignore')

files = sys.argv[1:]
filter_files = files[:-1]
gps_file = files[-1]



gps_df = pd.read_csv(gps_file, compression="gzip")
path_parts = gps_file.split("/")
# log_message(f"gps_file: {gps_file}", log_path)
year = path_parts[-2]
place_ = path_parts[-3]
place = "_".join(place_.split("_")[-2:])
os.makedirs(f"/home/fukui/workspace/TravelModeEstimation/logs/010_accuracy", exist_ok=True)
log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/010_accuracy/{place}_{year}.txt"
OUT_DIR = f"/home/data/fukui/processed/010_accuracy/{place}/{year}"
os.makedirs(OUT_DIR, exist_ok=True)

train_cluster_col = {
   "-1": "walk", 
   "0": "bus", 
   "1": "train", 
   "2": "bicycle", 
   "3": "car",
   "4": "bike"
   }
bus_cluster_col = {
   "-1": "walk", 
   "0": "bus", 
   "1": "car", 
   "2": "bicycle",
   "3": "bike"
   }
other_cluster_col = {
   "-1": "walk", 
   "0": "bicycle", 
   "1": "car",
   "2": "bike"
   }

gps_list = []
segment_list = []
for filter_file in filter_files:
    file_name = filter_file.split("/")[-1]
    gis_name = file_name.split("_")[0]
    if gis_name == "bus":
        bus_segment = pd.read_csv(filter_file, compression="gzip")
        bus_gps, bus_segment = mode_change(gps_df, bus_segment, bus_cluster_col, gis_name)
        gps_list.append(bus_gps)
        segment_list.append(bus_segment)
    elif gis_name == "train":
        train_segment = pd.read_csv(filter_file, compression="gzip")
        train_gps, train_segment = mode_change(gps_df, train_segment, train_cluster_col, gis_name)
        gps_list.append(train_gps)
        segment_list.append(train_segment)
    elif gis_name == "other":
        other_segment = pd.read_csv(filter_file, compression="gzip")
        other_gps, other_segment = mode_change(gps_df, other_segment, other_cluster_col, gis_name)
        gps_list.append(other_gps)
        segment_list.append(other_segment)


mode_gps_df = pd.concat(gps_list)
mode_gps_df.to_csv(f"{OUT_DIR}/mode_gps.csv.gz", index=False, compression="gzip")
mode_segment_df = pd.concat(segment_list)
mode_segment_df.to_csv(f"{OUT_DIR}/mode_segment.csv.gz", index=False, compression="gzip")