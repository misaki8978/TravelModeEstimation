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
from segment_analysis_func import mode_change, plot_ratio_by_user, plot_mode_analysis, segment_mode
from segment_analysis_func import plot_velocity_distribution_mode_bw, plot_multi_band_with_reference, accuracy_analysis


warnings.filterwarnings('ignore')

# message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/01_obsevation/log_01_gis_cluster.txt"

files = sys.argv[1:]

YEAR = files[0].split("/")[-2]
PLACE = files[0].split("/")[-3].split("_")[-2:]
PLACE = "_".join(PLACE)
# YEAR = files[0].split("/")[-3]
# PLACE = files[0].split("/")[-4].split("_")[-2:]
# PLACE = "_".join(PLACE)
# version = files[0].split("/")[-2]
# log_message(f"{PLACE}", message_path)
# OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/{PLACE}/{YEAR}/06_gis_frag/{version}"
# OUT_DIR_DATA = f"/home/data/fukui/processed/01_observation/{YEAR}/{version}"
# LOG_DIR = f"/home/fukui/workspace/TravelModeEstimation/logs/01_obsevation/{PLACE}_{YEAR}/{version}"
OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/{PLACE}/{YEAR}/06_gis_frag/pre"
OUT_DIR_DATA = f"/home/data/fukui/processed/01_observation/{YEAR}/pre"
LOG_DIR = f"/home/fukui/workspace/TravelModeEstimation/logs/01_obsevation/{PLACE}_{YEAR}/pre"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(OUT_DIR_DATA, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
#結果を見て適宜変更

message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/01_obsevation/{PLACE}_{YEAR}/pre/06_gis_frag.txt"

walk_cluster_col = {
   "-1": "walk"
}
train_cluster_col = {
   "-1": "walk", 
   "0": "car", 
   "1": "bus", 
   "2": "train", 
   "3": "bicycle",
   "4": "bike"
   }
bus_cluster_col = {
   "-1": "walk", 
   "0": "car", 
   "1": "bicycle", 
   "2": "bus",
   "3": "train"
   }
other_cluster_col = {
   "-1": "walk", 
   "0": "car", 
   "1": "bicycle",
   "2": "bike"
   }
mode_color = {
   "walk": "skyblue",
   "car": "red",
   "bus": "yellowgreen",
   "bicycle": "purple",
   "train": "#ffa500",
   "bike": "green"
}

mode_marker = {
   "walk": "D",       # 丸
   "car": "s",        # 四角
   "bus": "^",        # ダイヤ
   "bicycle": "x",       # 三角
   "train": "o",       # ×印,
   "bike": "v"     # 三角
}

# 地方都市 H27ver.
ref_mode_non_holiday={"bus":3.1, "train":4.3, "bicycle":16.1, "walk":17.8, "car":58.6} #平日
ref_mode_holiday={"bus":1.7, "train":2.6, "bicycle":11.1, "walk":12.5, "car":72.1} #休日

# 三大都市圏 H27ver.
# ref_mode_non_holiday={"バス":2.3, "鉄道":28.5, "二輪車":16.3, "徒歩・その他":21.5, "車":31.4} #平日
# ref_mode_holiday={"バス":2.0, "鉄道":16.3, "二輪車":12.3, "徒歩・その他":18.8, "車":50.6} #休日

df_list = []
for file in files:
    with gzip.open(file, 'rt') as f:
        gis = file.split("/")[-1].split("_")[0]
        df_segment = pd.read_csv(f, compression="gzip")
        df_segment = mode_change(df_segment, eval(f"{gis}_cluster_col"), gis)
        log_message(f"{gis}:{len(df_segment)}", message_path)
        log_message(f"{gis}:{df_segment['mode_label'].value_counts()}", message_path)
        df_list.append(df_segment)
df_segment = pd.concat(df_list)
plot_mode_analysis(df_segment, OUT_DIR, mode_color)


# plot_ratio_by_user(df_segment, OUT_DIR)

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

# log_message(f"{cluster_df.head()}", message_path)

# ---- 使い方（縦に４本：PT参照＋3本） ----
# plot_multi_band_with_reference(
#     cluster_df,
#     value_cols=["segment_count", "all_time", "all_distance"],
#     titles=["Segment Count", "All Time", "All Distance"],
#     color_map=mode_color,
#     ref_title="地方都市",
#     ref_mode_share_ja={"バス":2.2, "鉄道":24.4, "二輪車":15.5, "徒歩・その他":25.9, "車":31.9},
#     savepath=os.path.join(OUT_DIR, "multi_band_with_PT.png"),
#     figsize=(10, 6.2),  # 文字が詰まる場合は高さを調整
# )
# log_message("done", message_path)


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

df_move, df_mode_represent = segment_mode(df_segment)
# log_message(f"{df_mode_represent.head()}", message_path)
log_message(f"{df_mode_represent['mode_represent'].value_counts()}", message_path)
log_message(f"{df_mode_represent["is_holiday"].value_counts()}", message_path)
log_message(f"{df_mode_represent.query("is_holiday == True")["mode_represent"].value_counts()}", message_path)
log_message(f"{df_mode_represent.query("is_holiday == False")["mode_represent"].value_counts()}", message_path)
log_message(f"{df_move["n_segments"].mean()}", message_path)
log_message(f"holiday mean: {df_move.query("is_holiday == True")["n_segments"].mean()}", message_path)
log_message(f"weekday mean: {df_move.query("is_holiday == False")["n_segments"].mean()}", message_path)


holiday_df = df_mode_represent.query("is_holiday == True")["mode_represent"].value_counts()
non_holiday_df = df_mode_represent.query("is_holiday == False")["mode_represent"].value_counts()

holiday_rate = holiday_df / len(df_mode_represent.query("is_holiday == True"))
# log_message(f"{holiday_rate}", message_path)

non_holiday_rate = non_holiday_df / len(df_mode_represent.query("is_holiday == False"))
# log_message(f"{non_holiday_rate}", message_path)

holiday_rate_by_mode = pd.DataFrame({"mode_label": holiday_rate.index, "holidayrate": holiday_rate.values, "non-holidayrate": non_holiday_rate.values})
log_message(f"{holiday_rate_by_mode}", message_path)
holiday_rate_by_mode.to_csv(f"{OUT_DIR_DATA}/{PLACE}_represent_rate_by_mode.csv", index=False)

plot_multi_band_with_reference(
    holiday_rate_by_mode,
    value_cols=["non-holidayrate"],
    titles=["Our Study"],
    color_map=mode_color,
    ref_title="Person Trip Survey",
    ref_mode_share_ja=ref_mode_non_holiday,
    savepath=os.path.join(OUT_DIR, "multi_band_non_holiday_rate_by_mode.png"),
    figsize=(4,4),  # 文字が詰まる場合は高さを調整
)

plot_multi_band_with_reference(
    holiday_rate_by_mode,
    value_cols=["holidayrate"],
    titles=["Our Study"],
    color_map=mode_color,
    ref_title="Person Trip Survey",
    ref_mode_share_ja=ref_mode_holiday,
    savepath=os.path.join(OUT_DIR, "multi_band_holiday_rate_by_mode.png"),
    figsize=(4,4),  # 文字が詰まる場合は高さを調整
)

holiday_rate = holiday_rate_by_mode.set_index("mode_label")

holiday_accuracy = accuracy_analysis(holiday_rate["holidayrate"], ref_mode_holiday)
non_holiday_accuracy = accuracy_analysis(holiday_rate["non-holidayrate"], ref_mode_non_holiday)
log_message(f"holiday accuracy: {holiday_accuracy}", message_path)
log_message(f"non-holiday accuracy: {non_holiday_accuracy}", message_path)

log_message("done", message_path)

# 地方都市 H27ver.
#ref_mode_share_ja={"バス":3.1, "鉄道":4.3, "二輪車":16.1, "徒歩・その他":17.8, "車":58.6} 平日
#ref_mode_share_ja={"バス":1.7, "鉄道":2.6, "二輪車":11.1, "徒歩・その他":12.5, "車":72.1} 休日

# 三大都市圏 H27ver.
#ref_mode_share_ja={"バス":2.3, "鉄道":28.5, "二輪車":16.3, "徒歩・その他":21.5, "車":31.4} 平日
#ref_mode_share_ja={"バス":2.0, "鉄道":16.3, "二輪車":12.3, "徒歩・その他":18.8, "車":50.6} 休日