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
from segment_analysis_func import plot_mode_analysis, plot_velocity_distribution_mode_bw, segment_mode, plot_multi_band_with_reference

warnings.filterwarnings('ignore')

files = sys.argv[1:]
path_parts = files[0].split("/")
# log_message(f"gps_file: {gps_file}", log_path)
# year = path_parts[-2]
place = path_parts[-3]
log_path = f"/home/fukui/workspace/TravelModeEstimation/logs/010_accuracy/{place}.txt"

OUT_PLOT = f"/home/data/fukui/outputs/figures/010_05_mode_ratio_byyear/{place}"
os.makedirs(OUT_PLOT, exist_ok=True)

mode_color = {
   "walk": "skyblue",
   "car": "red",
   "bus": "yellowgreen",
   "two-wheeler": "purple",
   "train": "#ffa500",
   "bike": "green"
}

# 地方都市 H27ver.
# ref_mode_non_holiday={"bus":3.1, "train":4.3, "two-wheeler":16.1, "walk":17.8, "car":58.6} #平日
# ref_mode_holiday={"bus":1.7, "train":2.6, "two-wheeler":11.1, "walk":12.5, "car":72.1} #休日

# 三大都市圏 H27ver.
ref_mode_non_holiday={"bus":2.3, "train":28.5, "two-wheeler":16.3, "walk":21.5, "car":31.4} #平日
ref_mode_holiday={"bus":2.0, "train":16.3, "two-wheeler":12.3, "walk":18.8, "car":50.6} #休日

title_map = {
    "2019": "2019",
    "2020": "2020",
    "2021": "2021",
    "2022": "2022"
}

rate_list = []
for file in files:
    year = file.split("/")[-2].split("_")[0]
    rate = pd.read_csv(file, index_col=0)
    # index名を統一（ここ重要）
    rate.index.name = "mode_label"
    rate = rate.rename(columns={"holidayrate":f"{year}_holidayrate", "non-holidayrate": f"{year}_non-holidayrate"})
    rate_list.append(rate)

rate_df = pd.concat(rate_list, axis=1)
rate_df = rate_df.reset_index()
rate_df = rate_df.rename(columns={"index": "mode_label"})

dup_cols = rate_df.columns[rate_df.columns.duplicated()].tolist()
log_message(f"duplicated cols: {dup_cols}", log_path)

label_cols = rate_df.columns.tolist()
holiday_label_cols = [c for c in label_cols if c.endswith("_holidayrate")]
non_holiday_label_cols = [c for c in label_cols if c.endswith("_non-holidayrate")]


plot_multi_band_with_reference(
    rate_df,
    value_cols=non_holiday_label_cols,
    # version=version,
    titles=[title_map[c.replace("_non-holidayrate", "")] for c in non_holiday_label_cols],
    color_map=mode_color,
    ref_title="Person Trip Survey",
    ref_mode_share_ja=ref_mode_non_holiday,
    savepath=os.path.join(OUT_PLOT, f"multi_band_non_holiday_rate_by_mode.png"),
    figsize=(6,3),  # 文字が詰まる場合は高さを調整
)

plot_multi_band_with_reference(
    rate_df,
    value_cols=holiday_label_cols,
    titles=[title_map[c.replace("_holidayrate", "")] for c in holiday_label_cols],
    # version=version,
    color_map=mode_color,
    ref_title="Person Trip Survey",
    ref_mode_share_ja=ref_mode_holiday,
    savepath=os.path.join(OUT_PLOT, f"multi_band_holiday_rate_by_mode.png"),
    figsize=(6,3),  # 文字が詰まる場合は高さを調整
)