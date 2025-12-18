import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import seaborn as sns
import warnings
import geopandas as gpd
import datashader as ds
import datashader.transfer_functions as tf
from datashader.colors import inferno
from shapely.geometry import LineString, box, Point
import contextily as ctx
import shapely
import jpholiday

from datashader.transfer_functions import dynspread
from math import radians, sin, cos, sqrt, atan2


# 日本語フォントを全体設定
plt.rcParams['font.family'] = 'Noto Sans CJK JP'

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

warnings.filterwarnings('ignore')
# plt.rcParams['font.family'] = 'Meiryo'
message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_010_accuracy.txt"

def mode_change(df, segment_df, cluster_col, gis):
    df = df.merge(segment_df, on=['hashed_adid', 'segment_month_id'], how='inner')
    if gis == "other":
        df['mode_label'] = df['fuzzy_cluster_normal'].astype(str).map(cluster_col)
        segment_df['mode_label'] = segment_df['fuzzy_cluster_normal'].astype(str).map(cluster_col)
    else:
        df['mode_label'] = df['fuzzy_cluster_gis'].astype(str).map(cluster_col)
        segment_df['mode_label'] = segment_df['fuzzy_cluster_gis'].astype(str).map(cluster_col)
    return df, segment_df