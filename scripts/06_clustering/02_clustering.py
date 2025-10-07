#! /usr/bin/env python3
import os
import warnings
import pandas as pd
import sys
import gzip
import numpy as np

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from clustering_func import fuzzy_clustering, cluster_boxplot, Maximum_Membership_Degrees

warnings.filterwarnings('ignore')

files = sys.argv[1]  # 引数でファイルリストを受け取る


message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_06_clustering.txt"


YEAR = files.split("/")[-2]
PLACE = "_".join(files.split("/")[-3].split("_")[-2:])
OUT_DIR = f"/home/data/fukui/processed/06_02_{PLACE}/{YEAR}"
# log_message(f"{OUT_DIR}", message_path)
os.makedirs(OUT_DIR, exist_ok=True)
OUT_FIG = f"/home/data/fukui/outputs/figures/{PLACE}/{YEAR}/06_02_clustering/"
# log_message(f"{OUT_FIG}", message_path)
os.makedirs(OUT_FIG, exist_ok=True)

with gzip.open(files, 'rt') as f:
    df = pd.read_csv(f)\
        .query("n_points > 3 & max_speed < 30")\
        
    f.close()

log_message(f"{df["label"].value_counts()}", message_path)
log_message(f"{len(df)}", message_path)
log_message(f"{df['bearing_change_rate'].describe()}", message_path)

feature_cols = [
        'all_distance',
        'all_time',
        'mean_speed',
        'max_speed',
        'min_speed',
        'mean_accel',
        # 'bearing_change_rate'
    ]

gis_feature_cols = [
        'all_distance',
        'all_time',
        'mean_speed',
        'max_speed',
        'min_speed',
        'mean_accel',
        # 'bearing_change_rate',
        'buffer_train',
        'buffer_bus',
        # 'rail_flag',
        # 'bus_flag'
        ]



# log_message(f"{df.columns}", message_path)
# log_message(f"{df["bearing_change_rate"].describe()}", message_path)
df_normal = fuzzy_clustering(df, feature_cols, OUT_DIR, "normal")
df_clean = fuzzy_clustering(df_normal, gis_feature_cols, OUT_DIR, "gis")
log_message(f"{df_clean.columns}", message_path)
df_clean.to_csv(f"{OUT_DIR}/seg_fuzzy_cluster.csv.gz", index=False, compression="gzip")


#plot
cluster_boxplot(df_clean, feature_cols, OUT_FIG, "normal")
cluster_boxplot(df_clean, gis_feature_cols, OUT_FIG, "gis")
Maximum_Membership_Degrees(df_clean, OUT_FIG, "normal")
Maximum_Membership_Degrees(df_clean, OUT_FIG, "gis")