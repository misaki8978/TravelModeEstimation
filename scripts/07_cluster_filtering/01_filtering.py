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


log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/log_07_filtering.txt"
# --- 引数解析 ---
args = sys.argv[1:]
filter_file, gps_file = division_two_file(args)
log_message(f"files: {gps_file}", log_path)

path_parts = gps_file.split("/")
log_message(f"path_parts: {path_parts}", log_path)
year = path_parts[-2]
place_ = path_parts[-3]
place = "_".join(place_.split("_")[-2:])

OUT_DIR = f"/home/data/fukui/processed/07_01_{place}/{year}"
os.makedirs(OUT_DIR, exist_ok=True)



gps_df = pd.read_csv(gps_file, compression="gzip").query("label == 'non-walk'")
segment_df = pd.read_csv(filter_file, compression="gzip").query("label == 'non-walk'")
segment_df = segment_df[['hashed_adid', 'segment_month_id', 'fuzzy_cluster_normal', 'fuzzy_cluster_gis', 'max_membership_normal', 'max_membership_gis', 'rail_flag', 'bus_flag']]

gps_merge_df = gps_df.merge(segment_df, on=['hashed_adid', 'segment_month_id'], how='inner')

gps_merge_df.to_csv(f"{OUT_DIR}/gps_cluster_data.csv.gz", index=False, compression="gzip")

log_message(f"gps_merge_df.columns: {gps_merge_df.columns}", log_path)