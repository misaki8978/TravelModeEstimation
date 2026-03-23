#! /usr/bin/env python3
import os
import warnings
import pandas as pd
import sys
import gzip
import seaborn as sns
import geopandas as gpd
import datashader as ds
import datashader.transfer_functions as tf
from datashader.colors import inferno
from shapely.geometry import LineString
import contextily as ctx
import numpy as np
from datashader.transfer_functions import dynspread
from math import radians, sin, cos, sqrt, atan2

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from segment_analysis_func import plot_ratio_by_user, all_analysis
from gis import plot_gpspoint, getDistanceOfPoints
warnings.filterwarnings('ignore')

files = sys.argv[1]  # 引数でファイルリストを受け取る
# message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_03_seg_analysis.txt"
message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_00_segment_info.txt"


_path = os.path.dirname(files)
path_parts = _path.split("/")
log_message(f"{path_parts}", message_path)
# 最後の2つの要素を取得
year = path_parts[-1]
place = path_parts[-2]
# place = "_".join(place_.split("_")[2:])

OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/{place}/{year}/03_segment_analysis/"
# OUT_DATA = f"/home/data/fukui/processed/01_03_segment_analysis/{place}/{year}/"
os.makedirs(OUT_DIR, exist_ok=True)
# os.makedirs(OUT_DATA, exist_ok=True)

segment_df = pd.read_csv(files, compression="gzip")\
    .assign(
        date=lambda x: pd.to_datetime(x['date']),
        week_start=lambda x: x['date'].dt.date - pd.to_timedelta((x['date'].dt.weekday + 1) % 7, unit='d')
        )
log_message(f"segment_df: {segment_df.columns}", message_path)
log_message(f"segment_df: {segment_df['is_walk'].value_counts()}", message_path)
mapping = {
    # "all_distance": "Travel Distance",
    # "all_time": "Travel Time",
    # "mean_speed": "Average Speed",
    "max_speed": "Max Speed",
    # "min_speed": "Min Speed",
    # "stop_rate": "Stop Rate",
    # "mean_accel": "Mean Acceleration",
    # "bearing_change_rate": "Bearing Change Rate",
    # "buffer_train": "Proximity Rate to Railway",
    # "buffer_bus": "Proximity Rate to Bus Route",
}
plot_ratio_by_user(segment_df, OUT_DIR)
all_analysis(segment_df, OUT_DIR, mapping)
# seg_normal.to_csv(f"{OUT_DATA}/seg_normal.csv.gz", index=False, compression="gzip")
