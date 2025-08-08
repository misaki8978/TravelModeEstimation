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
from segment_analysis_func import plot_velocity_distribution, plot_mesh_heatmap_by_points, make_seg_features, make_normal_df, make_user_stats

warnings.filterwarnings('ignore')

files = sys.argv[1:]  # 引数でファイルリストを受け取る
# message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_03_seg_analysis.txt"
message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_00_segment_info.txt"


_path = os.path.dirname(files[0])
path_parts = _path.split("/")
# log_message(f"{path_parts}", message_path)
# 最後の2つの要素を取得
year = path_parts[-1]
place_ = path_parts[-2]
place = "_".join(place_.split("_")[2:])

OUT_DIR = f"/home/data/fukui/outputs/figures/{place}/{year}/03_segment_analysis/"
OUT_DATA = f"/home/data/fukui/processed/01_03_segment_analysis/{place}/{year}_weekly/"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(OUT_DATA, exist_ok=True)

df_list = []
for file in files:
    with gzip.open(file, 'rt') as f:
        df = pd.read_csv(f, parse_dates=['datetime'])\
                .assign(
                        latitude_anonymous=lambda x: np.radians(x["latitude_anonymous"]),
                        longitude_anonymous=lambda x: np.radians(x["longitude_anonymous"]),
                        )
        df_list.append(df)

df_segment = pd.concat(df_list)
# log_message(f"{df_segment.shape[0]} rows", message_path)
# log_message(f"{df_segment.select_dtypes(include='object').describe(include='all')}", message_path)
# log_message(f"{df_segment.select_dtypes(include='number').describe(include='all')}", message_path)
# seg_segment, seg_normal, seg_speed = make_seg_features(df_segment)
# log_message(f"{seg_normal.shape[0]} rows", message_path)
# log_message(f"{seg_normal.select_dtypes(include='object').describe(include='all')}", message_path)
# log_message(f"{seg_normal.select_dtypes(include='number').describe(include='all')}", message_path)
# plot_ratio_by_user(df_normal, OUT_DIR)
seg_normal = make_user_stats(df_segment, OUT_DIR)
log_message(f"{len(seg_normal)} rows", message_path)
log_message(f'{len(seg_normal.query('label == "non-walk"'))}', message_path)
log_message(f'{len(seg_normal.query('label == "walk"'))}', message_path)

normal_df, speed_df = make_normal_df(df_segment, seg_normal)
plot_velocity_distribution(seg_normal, OUT_DIR)

seg_normal.to_csv(f"{OUT_DATA}/seg_normal.csv.gz", index=False, compression="gzip")
# plot_speed_comparison(speed_df, OUT_DIR)

file_name = "all"
# plot_heatmap(df_normal, file_name, OUT_DIR) # 適宜変更

# plot_velocity_acceleration(seg_normal, OUT_DIR)
plot_mesh_heatmap_by_points(normal_df, file_name, OUT_DIR)