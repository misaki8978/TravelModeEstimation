#! /usr/bin/env python3
import os
import warnings
import pandas as pd
import sys
import gzip
import numpy as np

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from clustering_func import fuzzy_clustering

warnings.filterwarnings('ignore')

files = sys.argv[1]  # 引数でファイルリストを受け取る


message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_06_clustering.txt"


YEAR = "_".join(files.split("/")[-2].split("_")[:1])
PLACE = files.split("/")[-3]
OUT_DIR = f"/home/data/fukui/processed/06_02_{PLACE}/{YEAR}_weekly"
os.makedirs(OUT_DIR, exist_ok=True)

with gzip.open(files, 'rt') as f:
    df = pd.read_csv(f)\
        .query("label == 'non-walk'")\
        
    f.close()

feature_cols = [
        'mean_vel',
        'max_vel',
        'min_vel',
        'mean_acc',
        'total_distance',
        'duration_sec',
        'bearing_rate_rad'
    ]

df_clean = fuzzy_clustering(df, feature_cols, OUT_DIR)
log_message(f"{df_clean.columns}", message_path)
df_clean.to_csv(f"{OUT_DIR}/seg_cluster_fuzzy.csv.gz", index=False, compression="gzip")