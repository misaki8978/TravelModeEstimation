#! /usr/bin/env python3
import os
import warnings
import pandas as pd
import sys
import gzip
import osmnx as ox
from japanmap import get_data, pref_points, pref_names
from shapely.geometry import Polygon
import geopandas as gpd

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from clustering_func import map_plot
from accuracy_cal import mode_change

warnings.filterwarnings('ignore')

mode_gps_file = sys.argv[1]
path_parts = mode_gps_file.split("/")

year = path_parts[-2]
place_ = path_parts[-3]
place = "_".join(place_.split("_")[-2:])
OUT_DIR = f"/home/data/fukui/processed/010_accuracy/{place}/{year}"
os.makedirs(OUT_DIR, exist_ok=True)
log_path = f"/home/fukui/workspace/TravelModeEstimation/logs/010_accuracy/{place}_{year}.txt"
mode_gps_df = pd.read_csv(mode_gps_file, compression="gzip")
log_message(f"mode_gps_df: {len(mode_gps_df)}", log_path)
log_message(f"mode_gps_df: {mode_gps_df.columns.tolist()}", log_path)
# log_message(f"mode_gps_df: {mode_gps_df.head()}", log_path)
# サンプル数（各クラスごとに何個抽出するか）
SAMPLES_PER_CLASS = 40
mode_gps_df['unique_key'] = mode_gps_df['hashed_adid'].astype(str) + '_' + mode_gps_df['segment_month_id'].astype(str)
# セグメント単位でユニークなリストを作成（ポイント単位で抽出しないよう注意）
label_cols = [c for c in mode_gps_df.columns if 'mode_label' in c]
unique_segments = mode_gps_df[['unique_key', *label_cols]].drop_duplicates()
label_col = label_cols[0]

# 各クラスタからランダムに指定数だけセグメントIDを抽出
sampled_segments_df = unique_segments.groupby(label_col, group_keys=False).apply(
    lambda x: x.sample(min(len(x), SAMPLES_PER_CLASS), random_state=42) # random_state固定で再現性確保
)
log_message(f"label_col: {label_col}", log_path)
sampled_segments_df = sampled_segments_df[sampled_segments_df[label_col] == "train"]
sampled_ids = sampled_segments_df['unique_key'].tolist()
log_message(f"sampled_ids: {len(sampled_ids)}", log_path)

# 抽出されたIDに該当するGPSポイントのみを元のデータから取り出す
df_sampled = mode_gps_df[mode_gps_df['unique_key'].isin(sampled_ids)].copy()

# 判定結果を書き込むための空カラムを追加
sampled_segments_df['n_points'] = df_sampled['n_points']
sampled_segments_df['True_Label'] = '' # ここに手動で正解を書く
sampled_segments_df['Note'] = ''

# CSVまたはExcelとして保存（これを見て手作業を行う）
sampled_segments_df.to_csv(f'{OUT_DIR}/train_segment_labeling_sheet.csv', index=False)
df_sampled.to_csv(f'{OUT_DIR}/train_sampled_mode_gps.csv', index=False)
# print(f"判定用シートを作成しました: {len(sampled_segments_df)}件抽出")