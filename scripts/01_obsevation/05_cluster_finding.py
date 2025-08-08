#! /usr/bin/env python3
import os
import warnings
import pandas as pd
import sys
import gzip

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from clustering_func import cluster_pairplot, fuzzy_cluster_3Dplot, cluster_boxplot

warnings.filterwarnings('ignore')

files = sys.argv[1]  # 引数でファイルリストを受け取る


message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_05_cluster_finding.txt"


YEAR = files.split("/")[-2]
PLACE = files.split("/")[-3].split("_")[2:]
PLACE = "_".join(PLACE)
OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/{PLACE}/{YEAR}/05_cluster_finding"
os.makedirs(OUT_DIR, exist_ok=True)

with gzip.open(files, 'rt') as f:
    df = pd.read_csv(f)
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
# cluster_pairplot(df, feature_cols, OUT_DIR)
df = fuzzy_cluster_3Dplot(df, OUT_DIR)
df_07 = df.query("max_membership >= 0.9")
log_message(f"df_07.head: {df_07.head()}", message_path)
cluster_boxplot(df_07, feature_cols, OUT_DIR)








