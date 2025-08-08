import os
import sys
import gzip
import numpy as np
import pandas as pd
from glob import glob
import geopandas as gpd
import plotly.graph_objects as go
from shapely.geometry import Point
import matplotlib.pyplot as plt
import osmnx as ox

import warnings
warnings.filterwarnings('ignore')

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/log_09_gis.txt"

file = sys.argv[1]
place_plus = file.split('/')[-3]
place = '_'.join(place_plus.split('_')[-2:])
year = file.split('/')[-2]
month = file.split('/')[-1].split('_')[0]
log_message(f"place: {place}, year: {year}, month: {month}", log_path)
OUT_DIR = f"/home/data/fukui/outputs/figures/{place}/{year}/09_gis"
os.makedirs(OUT_DIR, exist_ok=True)

OUT_PROCESSED = f"/home/data/fukui/processed/09_01_{place}/{year}"
os.makedirs(OUT_PROCESSED, exist_ok=True)

# ---- 1. GPSデータの読み込み ----
gps_df = pd.read_csv(file, parse_dates=["datetime"], compression="gzip")
log_message(f"Loaded file {file}", log_path)

# GeoDataFrameに変換
gps_gdf = gpd.GeoDataFrame(
    gps_df,
    geometry=gpd.points_from_xy(gps_df.longitude_anonymous, gps_df.latitude_anonymous),
    crs="EPSG:4326"  # WGS84 (緯度経度)
)

# 地域を指定（例：東京）
place_name = "Nagasaki, Japan"
# 鉄道ネットワークを取得（例：東京都）
G = ox.graph_from_place(place_name, network_type='all_private', custom_filter='["railway"~"rail|subway|light_rail"]')

# ノードとエッジのGeoDataFrame化
nodes, edges = ox.graph_to_gdfs(G)


# 投影系を設定（バッファーはメートル単位で処理するため投影系が必要）
# 日本なら UTM zone 54N（例：EPSG:3099）などが使える。ここでは一例としてTokyo周辺のEPSG:3099を使用
edges_proj = edges.to_crs(epsg=3099)

# 線にバッファーをつける（例：50メートル）
buffered_edges = edges_proj.buffer(100)

# バッファーをGeoDataFrameに変換（プロットや保存のため）
buffered_gdf = gpd.GeoDataFrame(geometry=buffered_edges, crs=edges_proj.crs)

# 再び緯度経度に戻す（必要に応じて）
buffered_gdf = buffered_gdf.to_crs(epsg=4326)

# バス路線（route=bus）を含むオブジェクトを取得
tags = {"highway": "bus_stop"}
bus_stops = ox.features_from_place(place_name, tags)

# 線だけを取り出す（LineString）
bus_stops_points = bus_stops[bus_stops.geometry.type == "Point"]
bus_routes_proj = bus_stops_points.to_crs(epsg=3099)
buffered_bus = bus_routes_proj.buffer(30)
buffered_bus_gdf = gpd.GeoDataFrame(geometry=buffered_bus, crs=bus_routes_proj.crs)
buffered_bus_gdf = buffered_bus_gdf.to_crs(epsg=4326)
# fig, ax = plt.subplots(figsize=(12, 12))

# # 鉄道
# buffered_gdf.plot(ax=ax, color='lightblue', alpha=0.4, label='Rail Buffer')
# edges.plot(ax=ax, color='blue', linewidth=1, label='Rail')

# # バス
# buffered_bus_gdf.plot(ax=ax, color='orange', alpha=0.4, label='Bus Buffer')
# bus_stops_points.plot(ax=ax, color='red', markersize=3, label='Bus')

# plt.legend()
# plt.title("Rail and Bus Networks with Buffers")
# plt.savefig(f"{OUT_DIR}/railway_and_bus_network_with_buffer.png")



# 鉄道bufferを作成 (50m以内)
rail_buffer_union = buffered_gdf.buffer(100).unary_union

# バス停bufferを作成 (50m以内)
bus_stop_buffer_union = bus_stops_points.buffer(100).unary_union

# ---- 4. hashed_adid + segment_id 単位で集計 ----
results = []
group_cols = ["hashed_adid", "segment_id"]

for (adid, seg_id), trip_points in gps_gdf.groupby(group_cols):
    # 時系列順に並べる
    trip_points_sorted = trip_points.sort_values("datetime")

    # 鉄道との一致
    in_rail_buffer = trip_points_sorted.geometry.apply(lambda p: rail_buffer_union.contains(p))

    # バス停との一致
    in_bus_stop_buffer = trip_points_sorted.geometry.apply(lambda p: bus_stop_buffer_union.contains(p))

    # 始点・終点が鉄道buffer内かどうか
    start_point = trip_points_sorted.iloc[0].geometry
    end_point   = trip_points_sorted.iloc[-1].geometry
    start_in_rail = rail_buffer_union.contains(start_point)
    end_in_rail   = rail_buffer_union.contains(end_point)

    # 結果を保存
    results.append({
        "hashed_adid": adid,
        "segment_id": seg_id,
        "point_ratio_in_rail_buffer": in_rail_buffer.mean(),       # 鉄道近接割合
        "point_ratio_in_bus_stop_buffer": in_bus_stop_buffer.mean(),  # バス停近接割合
        "start_in_rail_buffer": start_in_rail,
        "end_in_rail_buffer": end_in_rail
    })

# ---- 5. 結果のDataFrame化 ----
trip_results_df = pd.DataFrame(results)

trip_results_df.to_csv(f"{OUT_PROCESSED}/{month}_trip_results.csv", index=False)