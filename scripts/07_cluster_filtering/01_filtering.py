#! /usr/bin/env python3
import os
import sys
import pandas as pd
import gzip
from datetime import datetime, timedelta
import warnings
import numpy as np

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from file_open import division_two_file

warnings.filterwarnings('ignore')



# --- 引数解析 ---
gps_file = sys.argv[1]
filter_files = sys.argv[2:]
# filter_file, gps_file = division_two_file(args)
# log_message(f"files: {gps_file}", log_path)

for filter_file in filter_files:
    file_name = filter_file.split("/")[-1]
    if file_name.split("_")[0] == "bus":
        bus_filter_file = filter_file
    elif file_name.split("_")[0] == "train":
        train_filter_file = filter_file
    elif file_name.split("_")[0] == "other":
        other_filter_file = filter_file

path_parts = gps_file.split("/")
# log_message(f"path_parts: {path_parts}", log_path)
year = path_parts[-2]
place_ = path_parts[-3]
place = "_".join(place_.split("_")[-2:])
os.makedirs(f"/home/fukui/workspace/TravelModeEstimation/logs/07_cluster_filtering", exist_ok=True)
log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/07_cluster_filtering/{place}_{year}.txt"
OUT_DIR = f"/home/data/fukui/processed/07_01/{place}/{year}"
os.makedirs(OUT_DIR, exist_ok=True)



gps_df = pd.read_csv(gps_file, compression="gzip").query("label == 'non-walk'")
bus_segment_df = pd.read_csv(bus_filter_file, compression="gzip").query("label == 'non-walk'")[['hashed_adid', 'segment_month_id', 'fuzzy_cluster_normal', 'fuzzy_cluster_gis', 'max_membership_normal', 'max_membership_gis', 'rail_flag', 'bus_flag']]
train_segment_df = pd.read_csv(train_filter_file, compression="gzip").query("label == 'non-walk'")[['hashed_adid', 'segment_month_id', 'fuzzy_cluster_normal', 'fuzzy_cluster_gis', 'max_membership_normal', 'max_membership_gis', 'rail_flag', 'bus_flag']]
other_segment_df = pd.read_csv(other_filter_file, compression="gzip").query("label == 'non-walk'")[['hashed_adid', 'segment_month_id', 'fuzzy_cluster_normal', 'fuzzy_cluster_gis', 'max_membership_normal', 'max_membership_gis', 'rail_flag', 'bus_flag']]

bus_merge_df = gps_df.merge(bus_segment_df, on=['hashed_adid', 'segment_month_id'], how='inner')
train_merge_df = gps_df.merge(train_segment_df, on=['hashed_adid', 'segment_month_id'], how='inner')
other_merge_df = gps_df.merge(other_segment_df, on=['hashed_adid', 'segment_month_id'], how='inner')

bus_merge_df.to_csv(f"{OUT_DIR}/bus_cluster_data.csv.gz", index=False, compression="gzip")
train_merge_df.to_csv(f"{OUT_DIR}/train_cluster_data.csv.gz", index=False, compression="gzip")
other_merge_df.to_csv(f"{OUT_DIR}/other_cluster_data.csv.gz", index=False, compression="gzip")

log_message(f"bus_merge_df: {len(bus_merge_df)}points", log_path)
log_message(f"train_merge_df: {len(train_merge_df)}points", log_path)
log_message(f"other_merge_df: {len(other_merge_df)}points", log_path)