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
from segment_analysis_func import plot_mode_analysis, plot_velocity_distribution_mode_bw, segment_mode, plot_multi_band_with_reference, plot_multi_band_with_reference_version

warnings.filterwarnings('ignore')

files = sys.argv[1:]
filter_files = files[:-1]
gps_file = files[-1]

version = filter_files[0].split("/")[-2]

gps_df = pd.read_csv(gps_file, compression="gzip")
path_parts = gps_file.split("/")

year = path_parts[-2]
place_ = path_parts[-3]
place = "_".join(place_.split("_")[-2:])
os.makedirs(f"/home/fukui/workspace/TravelModeEstimation/logs/010_accuracy", exist_ok=True)
log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/010_accuracy/{place}_{year}.txt"
OUT_DIR = f"/home/data/fukui/processed/010_accuracy/{place}/{year}"
os.makedirs(OUT_DIR, exist_ok=True)
OUT_PLOT = f"/home/data/fukui/outputs/figures/{place}/{year}/010_accuracy/{version}"
os.makedirs(OUT_PLOT, exist_ok=True)
log_message(f"gps_df: {gps_df.head()}", log_path)

walk_cluster_col = {
   "-1": "walk"
    }
train_cluster_col = {
   "-1": "walk", 
   "0": "train", 
   "1": "car", 
   "2": "train", 
   "3": "bus",
   "4": "bike"
   }
bus_cluster_col = {
   "-1": "walk", 
   "0": "bus", 
   "1": "two-wheeler", 
   "2": "car",
   "3": "car"
   }
other_cluster_col = {
   "-1": "walk", 
   "0": "car", 
   "1": "two-wheeler",
   "2": "bike"
   }
normal_cluster_col = {
   "-1": "walk", 
   "0": "car", 
   "1": "two-wheeler", 
   "2": "train", 
   "3": "bus"
   }
normal_gis_cluster_col = {  # 06_02_clustering/normal
    "-1": "walk",
    "0": "car",
    "1": "two-wheeler",
    "2": "bus",
    "3": "bus"
}
gps_list = []
segment_list = []
for filter_file in filter_files:
    file_name = filter_file.split("/")[-1]
    gis_name = file_name.split("_")[0]
    if gis_name == "bus":
        bus_segment = pd.read_csv(filter_file, compression="gzip")
        log_message(f"bus_segment: {bus_segment.head()}", log_path)
        bus_gps, bus_segment = mode_change(gps_df, bus_segment, bus_cluster_col, "None", gis_name, version)
        gps_list.append(bus_gps)
        segment_list.append(bus_segment)
    elif gis_name == "train":
        train_segment = pd.read_csv(filter_file, compression="gzip")
        train_gps, train_segment = mode_change(gps_df, train_segment, train_cluster_col, "None", gis_name, version)
        gps_list.append(train_gps)
        segment_list.append(train_segment)
    elif gis_name == "walk":
        walk_segment = pd.read_csv(filter_file, compression="gzip")
        walk_gps, walk_segment = mode_change(gps_df, walk_segment, walk_cluster_col, normal_gis_cluster_col, gis_name, version)
        gps_list.append(walk_gps)
        segment_list.append(walk_segment)
    elif gis_name == "other":
        other_segment = pd.read_csv(filter_file, compression="gzip")
        other_gps, other_segment = mode_change(gps_df, other_segment, other_cluster_col, "None", gis_name, version)
        gps_list.append(other_gps)
        segment_list.append(other_segment)
    elif gis_name == "normal":
        normal_segment = pd.read_csv(filter_file, compression="gzip")
        normal_gps, normal_segment = mode_change(gps_df, normal_segment, normal_cluster_col, normal_gis_cluster_col, gis_name, version)
        gps_list.append(normal_gps)
        segment_list.append(normal_segment)


mode_gps_df = pd.concat(gps_list)
mode_gps_df.to_csv(f"{OUT_DIR}/{version}_mode_gps.csv.gz", index=False, compression="gzip")
mode_segment_df = pd.concat(segment_list)
mode_segment_df.to_csv(f"{OUT_DIR}/{version}_mode_segment.csv.gz", index=False, compression="gzip")


mode_color = {
   "walk": "skyblue",
   "car": "red",
   "bus": "yellowgreen",
   "two-wheeler": "purple",
   "train": "#ffa500",
   "bike": "green"
}

mode_marker = {
   "walk": "D",       # 丸
   "car": "s",        # 四角
   "bus": "^",        # ダイヤ
   "two-wheeler": "x",       # 三角
   "train": "o",       # ×印,
   "bike": "v"     # 三角
}

# 地方都市 H27ver.
ref_mode_non_holiday={"bus":3.1, "train":4.3, "two-wheeler":16.1, "walk":17.8, "car":58.6} #平日
ref_mode_holiday={"bus":1.7, "train":2.6, "two-wheeler":11.1, "walk":12.5, "car":72.1} #休日

# 三大都市圏 H27ver.
# ref_mode_non_holiday={"bus":2.3, "train":28.5, "two-wheeler":16.3, "walk":21.5, "car":31.4} #平日
# ref_mode_holiday={"bus":2.0, "train":16.3, "two-wheeler":12.3, "walk":18.8, "car":50.6} #休日

plot_mode_analysis(mode_segment_df, OUT_PLOT, mode_color, version)
# plot_mode_analysis(mode_segment_df, OUT_PLOT, mode_color, version+"_gis") #normal ver.のみ
plot_velocity_distribution_mode_bw(mode_segment_df, OUT_PLOT, mode_marker, version)
# plot_velocity_distribution_mode_bw(mode_segment_df, OUT_PLOT, mode_marker, version+"_gis") #normal ver.のみ
mode_segment_df["segment_key"] = mode_segment_df["hashed_adid"].astype(str) + "_" + mode_segment_df["segment_month_id"].astype(str)
# cluster_df = df_segment.groupby("mode_label")\
#                      .agg(
#                         segment_count = ("segment_key", "count"),
#                         all_time = ("all_time", "sum"),
#                         all_distance = ("all_distance", "sum"),
#                         ratio_buffer_train=("buffer_train", lambda x: (x >= 0.7).mean()),
#                         ratio_buffer_bus=("buffer_bus", lambda x: (x >= 0.7).mean())
#                      )\
#                      .reset_index()

df_move, df_mode_represent = segment_mode(mode_segment_df, version)
# df_move_normal, df_mode_represent_normal = segment_mode(mode_segment_df, f"mode_label_{version}_gis") #normal ver.のみ
holiday_df = df_mode_represent.query("is_holiday == True")["mode_represent"].value_counts()
non_holiday_df = df_mode_represent.query("is_holiday == False")["mode_represent"].value_counts()

holiday_rate = holiday_df / len(df_mode_represent.query("is_holiday == True"))
# log_message(f"{holiday_rate}", message_path)

non_holiday_rate = non_holiday_df / len(df_mode_represent.query("is_holiday == False"))
# log_message(f"{non_holiday_rate}", message_path)

holiday_rate_by_mode = pd.DataFrame({f"mode_label_{version}": holiday_rate.index, "holidayrate": holiday_rate.values, "non-holidayrate": non_holiday_rate.values})
log_message(f"{holiday_rate_by_mode}", log_path)

plot_multi_band_with_reference_version(
    holiday_rate_by_mode,
    value_cols=["non-holidayrate"],
    version=version,
    titles=["Our Study"],
    color_map=mode_color,
    ref_title="Person Trip Survey",
    ref_mode_share_ja=ref_mode_non_holiday,
    savepath=os.path.join(OUT_PLOT, f"{version}_multi_band_non_holiday_rate_by_mode.png"),
    figsize=(6,3),  # 文字が詰まる場合は高さを調整
)

plot_multi_band_with_reference_version(
    holiday_rate_by_mode,
    value_cols=["holidayrate"],
    version=version,
    titles=["Our Study"],
    color_map=mode_color,
    ref_title="Person Trip Survey",
    ref_mode_share_ja=ref_mode_holiday,
    savepath=os.path.join(OUT_PLOT, f"{version}_multi_band_holiday_rate_by_mode.png"),
    figsize=(6,3),  # 文字が詰まる場合は高さを調整
)

holiday_rate_by_mode.to_csv(f"{OUT_DIR}/{version}_holiday_rate_by_mode.csv", index=False)

if version == "normal":
    holiday_df = df_mode_represent.query("is_holiday == True")["mode_represent_gis"].value_counts()
    non_holiday_df = df_mode_represent.query("is_holiday == False")["mode_represent_gis"].value_counts()

    holiday_rate = holiday_df / len(df_mode_represent.query("is_holiday == True"))
    # log_message(f"{holiday_rate}", message_path)

    non_holiday_rate = non_holiday_df / len(df_mode_represent.query("is_holiday == False"))
    # log_message(f"{non_holiday_rate}", message_path)

    holiday_rate_by_mode = pd.DataFrame({f"mode_label_{version}_gis": holiday_rate.index, "holidayrate": holiday_rate.values, "non-holidayrate": non_holiday_rate.values})
    log_message(f"{holiday_rate_by_mode}", log_path)

    plot_multi_band_with_reference_version(
        holiday_rate_by_mode,
        value_cols=["non-holidayrate"],
        version=version+"_gis",
        titles=["Our Study"],
        color_map=mode_color,
        ref_title="Person Trip Survey",
        ref_mode_share_ja=ref_mode_non_holiday,
        savepath=os.path.join(OUT_PLOT, f"{version}_gis_multi_band_non_holiday_rate_by_mode.png"),
        figsize=(6,3),  # 文字が詰まる場合は高さを調整
    )

    plot_multi_band_with_reference_version  (
        holiday_rate_by_mode,
        value_cols=["holidayrate"],
        titles=["Our Study"],
        version=version+"_gis",
        color_map=mode_color,
        ref_title="Person Trip Survey",
        ref_mode_share_ja=ref_mode_holiday,
        savepath=os.path.join(OUT_PLOT, f"{version}_gis_multi_band_holiday_rate_by_mode.png"),
        figsize=(6,3),  # 文字が詰まる場合は高さを調整
    )
    holiday_rate_by_mode.to_csv(f"{OUT_DIR}/{version}_gis_holiday_rate_by_mode.csv", index=False)

log_message("done", log_path)