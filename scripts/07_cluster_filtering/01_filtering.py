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

warnings.filterwarnings('ignore')


log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/log_07_filtering.txt"
# --- 引数解析 ---
args = sys.argv[1:]
split_idx = args.index('--')
filter_file = args[0]
files = args[2:]
log_message(f"files: {files}", log_path)

path_parts = files[0].split("/")
log_message(f"path_parts: {path_parts}", log_path)
year = path_parts[-2]
place_ = path_parts[-3]
place = "_".join(place_.split("_")[2:])

OUT_DIR = f"/home/data/fukui/processed/07_01_{place}/{year}"
os.makedirs(OUT_DIR, exist_ok=True)

# --- ファイル読み込み ---
df_filter = pd.read_csv(filter_file)

for file in files:
    out_file = file.split("/")[-1]
    out_file = out_file.replace('.csv.gz', '_filtered.csv.gz')
    with gzip.open(file, 'rt') as f:
        df = pd.read_csv(f)\
            .assign(
                latitude=lambda x: np.degrees(x["latitude_anonymous"]),
                longitude=lambda x: np.degrees(x["longitude_anonymous"]),
            )
        # dfとdf_filterをマージして、fuzzy_clusterとmax_membershipを追加
        df = df.merge(
            df_filter[['hashed_adid', 'segment_id', 'fuzzy_cluster', 'max_membership']],
            on=['hashed_adid', 'segment_id'],
            how='inner'
        ).drop(columns=['latitude_anonymous', 'longitude_anonymous'])

        log_message(f"df緯度経度のソート後: {df['latitude'].min()} {df['latitude'].max()} {df['longitude'].min()} {df['longitude'].max()}", log_path)
        df.to_csv(f'{OUT_DIR}/{out_file}', index=False, compression='gzip')
        



