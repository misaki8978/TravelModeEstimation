#! /usr/bin/env python3
import os
import warnings
import pandas as pd
import sys
import gzip
import numpy as np

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from clustering_func import fuzzy_clustering, normal_cluster_boxplot, normal_Maximum_Membership_Degrees, data_sepa, start_clustering

warnings.filterwarnings('ignore')

files = sys.argv[1]  # 引数でファイルリストを受け取る

YEAR = files.split("/")[-2]
PLACE = "_".join(files.split("/")[-3].split("_")[-2:])
OUT_DIR = f"/home/data/fukui/processed/06_02/{PLACE}/{YEAR}/normal"

os.makedirs(OUT_DIR, exist_ok=True)
OUT_FIG = f"/home/data/fukui/outputs/figures/{PLACE}/{YEAR}/06_02_clustering/normal"
os.makedirs(OUT_FIG, exist_ok=True)
os.makedirs(f"/home/fukui/workspace/TravelModeEstimation/logs/06_clustering", exist_ok=True)
message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/06_clustering/{PLACE}_{YEAR}.txt"
with gzip.open(files, 'rt') as f:
    df = pd.read_csv(f)\
        .query("n_points > 3 & max_speed <= 32 & all_time <= 4*60*60 & all_distance <= 40000")\
        
    f.close()

df_walk = df.query("is_walk == 1")\
            .assign(
                fuzzy_cluster_gis = -1,
                fuzzy_cluster_normal = -1
            )
log_message(f"{len(df_walk)} walk segments", message_path)
df_walk.to_csv(f"{OUT_DIR}/walk_gis_cluster.csv.gz", index=False, compression="gzip")

feature_cols = [
        'all_distance',
        'all_time',
        'mean_speed',
        'max_speed',
        'min_speed',
        'stop_rate',
        'mean_accel',
        'bearing_change_rate'
    ]

gis_feature_cols = [
        'all_distance',
        'all_time',
        'mean_speed',
        'max_speed',
        'min_speed',
        'mean_accel',
        'bearing_change_rate',
        'stop_rate',
        'buffer_train',
        'buffer_bus',
        # 'train_stop_proximity_rate',
        # 'bus_stop_proximity_rate',
        # 'rail_flag',
        # 'bus_flag'
        ]
df_nonwalk = df.query("is_walk == 0")
n_clusters = 4
df_normal = fuzzy_clustering(df_nonwalk, feature_cols, OUT_DIR, "normal", n_clusters)
df_clean = fuzzy_clustering(df_normal, gis_feature_cols, OUT_DIR, "gis", n_clusters)

df_clean.to_csv(f"{OUT_DIR}/normal_gis_cluster.csv.gz", index=False, compression="gzip")

normal_cluster_boxplot(df_clean, feature_cols, OUT_FIG, "normal")
normal_cluster_boxplot(df_clean, gis_feature_cols, OUT_FIG, "gis")
normal_Maximum_Membership_Degrees(df_clean, OUT_FIG, "normal")
normal_Maximum_Membership_Degrees(df_clean, OUT_FIG, "gis")