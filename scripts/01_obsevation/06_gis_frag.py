import os
import pandas as pd
import matplotlib.pyplot as plt
import gzip
import sys
import warnings
import numpy as np

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from clustering_func import cluster_analysis
from segment_analysis_func import mode_change, plot_ratio_by_user
from segment_analysis_func import plot_velocity_distribution_mode_bw, plot_multi_band_with_reference


warnings.filterwarnings('ignore')

message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_gis_cluster.txt"

files = sys.argv[1]

YEAR = files.split("/")[-2]
PLACE = files.split("/")[-3].split("_")[-2:]
PLACE = "_".join(PLACE)
OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/06_gis_frag/{PLACE}/{YEAR}"
os.makedirs(OUT_DIR, exist_ok=True)

df_segment = pd.read_csv(files, compression="gzip")

log_message(f"{df_segment.columns}", message_path)

#結果を見て適宜変更
cluster_col = {
   "-1": "walk", 
   "0": "train", 
   "1": "bike", 
   "2": "car", 
   "3": "bus"
   }
mode_color = {
   "walk": "skyblue",
   "car": "red",
   "bus": "yellowgreen",
   "bike": "purple",
   "train": "#ffa500"
}

mode_marker = {
   "walk": "D",       # 丸
   "car": "s",        # 四角
   "bus": "^",        # ダイヤ
   "bike": "x",       # 三角
   "train": "o"       # ×印
}

df_segment = mode_change(df_segment, cluster_col)
log_message(f"{df_segment.columns}", message_path)
log_message(f"{df_segment['mode_label'].value_counts()}", message_path)
# mode_analysis(df_segment, OUT_DIR, mode_color)
plot_ratio_by_user(df_segment, OUT_DIR)

plot_velocity_distribution_mode_bw(df_segment, OUT_DIR, mode_marker)
df_segment["segment_key"] = df_segment["hashed_adid"].astype(str) + "_" + df_segment["segment_month_id"].astype(str)
cluster_df = df_segment.groupby("mode_label")\
                     .agg(
                        segment_count = ("segment_key", "count"),
                        all_time = ("all_time", "sum"),
                        all_distance = ("all_distance", "sum"),
                        ratio_buffer_train=("buffer_train", lambda x: (x >= 0.7).mean()),
                        ratio_buffer_bus=("buffer_bus", lambda x: (x >= 0.7).mean())
                     )\
                     .reset_index()

log_message(f"{cluster_df.head()}", message_path)

# ---- 使い方（縦に４本：PT参照＋3本） ----
plot_multi_band_with_reference(
    cluster_df,
    value_cols=["segment_count", "all_time", "all_distance"],
    titles=["Segment Count", "All Time", "All Distance"],
    color_map=mode_color,
    ref_title="地方都市",
    ref_mode_share_ja={"バス":1.3, "鉄道":2.2, "自転車":8.1, "徒歩・その他":12.7, "車":75.7},
    savepath=os.path.join(OUT_DIR, "multi_band_with_PT.png"),
    figsize=(10, 6.2),  # 文字が詰まる場合は高さを調整
)
log_message("done", message_path)


# plot_multi_band_with_reference(
#     cluster_df,
#     value_cols=["segment_count"],
#     titles=["Segment Count"],
#     color_map=mode_color,
#     ref_title="地方都市",
#     ref_mode_share_ja={"バス":1.3, "鉄道":2.2, "自転車":8.1, "徒歩・その他":12.7, "車":75.7},
#     savepath=os.path.join(OUT_DIR, "segment_count_with_PT.png"),
#     figsize=(10, 4.5),  # 文字が詰まる場合は高さを調整
# )