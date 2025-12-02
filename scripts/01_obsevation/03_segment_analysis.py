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
# from segment_analysis_func import plot_velocity_distribution, plot_mesh_heatmap_by_points, make_seg_features, make_normal_df, make_user_stats, plot_ratio_by_user
from gis import plot_gpspoint
warnings.filterwarnings('ignore')

files = sys.argv[1:]  # 引数でファイルリストを受け取る
# message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_03_seg_analysis.txt"
message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_00_segment_info.txt"


_path = os.path.dirname(files[0])
path_parts = _path.split("/")
# log_message(f"{path_parts}", message_path)
# 最後の2つの要素を取得
year = path_parts[-1].split("_")[0]
place = path_parts[-2]
# place = "_".join(place_.split("_")[2:])

OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/{place}/{year}/03_segment_analysis/"
OUT_DATA = f"/home/data/fukui/processed/01_03_segment_analysis/{place}/{year}/"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(OUT_DATA, exist_ok=True)

df_list = []
for file in files:
    with gzip.open(file, 'rt') as f:
        month = file.split('/')[-1].split('_')[0]
        df = pd.read_csv(f, parse_dates=['datetime'])\
                .assign(
                        latitude=lambda x: np.radians(x["latitude_anonymous"]),
                        longitude=lambda x: np.radians(x["longitude_anonymous"]),
                        segment_month_id = lambda x: month + "_" + x['segment_id'].astype(str),
                        move_id = lambda x: x['segment_id'].astype(str) + "_" + x['datetime'].astype(str),
                        )
        df_list.append(df)

df_segment = pd.concat(df_list)
# plot_gpspoint(df_segment, OUT_DIR)
df_segment_id = df_segment.groupby(['hashed_adid', 'segment_month_id'])\
        .agg(
            label = ('is_walk', 'first'),
        #     label_cer = ('label_cer', 'first'),
        #     P_speed = ('P_speed', 'first'),
        #     speed = ('speed', 'first'),
        #     acceleration = ('acceleration', 'first'),
        )
log_message(f"{place} {year} : {df_segment_id['label'].value_counts()}", message_path)
# log_message(f"{place} {year} : {df_segment.groupby('segment_month_id')['label'].value_counts()}", message_path)
# log_message(f"all points: {len(df_segment)} rows", message_path)
# seg_normal = make_user_stats(df_segment, OUT_DIR)
# log_message(f"{len(seg_normal)} rows", message_path)
# log_message(f'{len(seg_normal.query('label == "non-walk"'))}', message_path)
# log_message(f'{len(seg_normal.query('label == "walk"'))}', message_path)

# normal_df, speed_df = make_normal_df(df_segment, seg_normal)
# plot_velocity_distribution(seg_normal, OUT_DIR)
# plot_ratio_by_user(seg_normal, OUT_DIR)
# seg_normal.to_csv(f"{OUT_DATA}/seg_normal.csv.gz", index=False, compression="gzip")
