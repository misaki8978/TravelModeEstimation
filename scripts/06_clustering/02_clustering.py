#! /usr/bin/env python3
import os
import warnings
import pandas as pd
import sys
import gzip
import numpy as np

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from clustering_func import fuzzy_clustering, cluster_boxplot, Maximum_Membership_Degrees, data_sepa, start_clustering

warnings.filterwarnings('ignore')

files = sys.argv[1]  # 引数でファイルリストを受け取る





YEAR = files.split("/")[-2]
PLACE = "_".join(files.split("/")[-3].split("_")[-2:])
OUT_DIR = f"/home/data/fukui/processed/06_02/{PLACE}/{YEAR}/stops"
# OUT_DIR = f"/home/data/fukui/processed/06_02_{PLACE}/{YEAR}/"

# log_message(f"{OUT_DIR}", message_path)
os.makedirs(OUT_DIR, exist_ok=True)
OUT_FIG = f"/home/data/fukui/outputs/figures/{PLACE}/{YEAR}/06_02_clustering/stops"
# OUT_FIG = f"/home/data/fukui/outputs/figures/{PLACE}/{YEAR}/06_02_clustering/pre"

# log_message(f"{OUT_FIG}", message_path)
os.makedirs(OUT_FIG, exist_ok=True)
os.makedirs(f"/home/fukui/workspace/TravelModeEstimation/logs/06_clustering", exist_ok=True)
message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/06_clustering/{PLACE}_{YEAR}.txt"
with gzip.open(files, 'rt') as f:
    df = pd.read_csv(f)\
        .query("n_points > 3 & max_speed <= 30 & all_time <= 4*60*60 & all_distance <= 40000")\
        
    f.close()
# log_message(f"{df.columns}", message_path)

# log_message(f"{df["label"].value_counts()}", message_path)
# log_message(f"{len(df)}", message_path)
# log_message(f"{df['buffer_bus'].describe()}", message_path)
df_walk = df.query("is_walk == 1")\
            .assign(
                fuzzy_cluster_gis = -1
            )
log_message(f"{len(df_walk)} walk segments", message_path)
df_walk.to_csv(f"{OUT_DIR}/walk_gis_cluster.csv.gz", index=False, compression="gzip")

#路線適合確率0.7を境目にデータを3つに分割
df_train, df_bus, df_other = data_sepa(df, 0.7)
log_message(f"near bus segment: {len(df_bus)}", message_path)
log_message(f"near train segment: {len(df_train)}", message_path)
log_message(f"other segment: {len(df_other)}", message_path)

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
        'train_stop_proximity_rate',
        'bus_stop_proximity_rate',
        # 'rail_flag',
        # 'bus_flag'
        ]
log_message(f"{len(df_bus)}", message_path)
df_bus = df_bus.dropna(subset=gis_feature_cols)
log_message(f"{len(df_bus)}", message_path)
df_normal_bus, df_clean_bus = start_clustering(df_bus, gis_feature_cols, feature_cols, 3, OUT_DIR)
df_normal_train, df_clean_train = start_clustering(df_train, gis_feature_cols, feature_cols, 4, OUT_DIR)
df_normal_other, df_clean_other = start_clustering(df_other, gis_feature_cols, feature_cols, 2, OUT_DIR)

log_message(f"{df_clean_bus.columns}", message_path)

df_clean_bus.to_csv(f"{OUT_DIR}/bus_gis_cluster.csv.gz", index=False, compression="gzip")
df_clean_train.to_csv(f"{OUT_DIR}/train_gis_cluster.csv.gz", index=False, compression="gzip")
df_clean_other.to_csv(f"{OUT_DIR}/other_gis_cluster.csv.gz", index=False, compression="gzip")




#plot
cluster_boxplot(df_clean_bus, feature_cols, OUT_FIG, "normal_bus")
cluster_boxplot(df_clean_train, feature_cols, OUT_FIG, "normal_train")
cluster_boxplot(df_clean_other, feature_cols, OUT_FIG, "normal_other")

cluster_boxplot(df_clean_bus, gis_feature_cols, OUT_FIG, "gis_bus")
cluster_boxplot(df_clean_train, gis_feature_cols, OUT_FIG, "gis_train")
cluster_boxplot(df_clean_other, gis_feature_cols, OUT_FIG, "gis_other")

Maximum_Membership_Degrees(df_clean_bus, OUT_FIG, "normal_bus")
Maximum_Membership_Degrees(df_clean_train, OUT_FIG, "normal_train")
Maximum_Membership_Degrees(df_clean_other, OUT_FIG, "normal_other")

Maximum_Membership_Degrees(df_clean_bus, OUT_FIG, "gis_bus")
Maximum_Membership_Degrees(df_clean_train, OUT_FIG, "gis_train")
Maximum_Membership_Degrees(df_clean_other, OUT_FIG, "gis_other")