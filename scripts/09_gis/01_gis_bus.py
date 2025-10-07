import os
import sys
import gzip
import numpy as np
import pandas as pd
from glob import glob
import geopandas as gpd
import plotly.graph_objects as go
from shapely.geometry import Point
from shapely.ops import unary_union
import matplotlib.pyplot as plt
import osmnx as ox

import warnings
warnings.filterwarnings('ignore')

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/log_09_gis.txt"

files = sys.argv[1:]

split_idx = files.index('--')
file, gis_file = files[split_idx - 1], files[split_idx + 1]
log_message(f"file: {file}, gis_file: {gis_file}", log_path)
place_plus = file.split('/')[-3]
place = '_'.join(place_plus.split('_')[-3:-1])
year = file.split('/')[-2]
month = file.split('/')[-1].split('_')[0]
log_message(f"place: {place}, year: {year}, month: {month}", log_path)
OUT_DIR = f"/home/data/fukui/outputs/figures/{place}/{year}/09_gis"
os.makedirs(OUT_DIR, exist_ok=True)

OUT_PROCESSED = f"/home/data/fukui/processed/09_01_{place}_replay/{year}"
os.makedirs(OUT_PROCESSED, exist_ok=True)

place_name = "Nagasaki, Japan"
projected_crs = "EPSG:32652"

# ---- 1. GPSデータの読み込み ----
gps_df = pd.read_csv(file, parse_dates=["datetime"], compression="gzip")
log_message(f"Loaded file {file}", log_path)

# GeoDataFrameに変換
gps_gdf = gpd.GeoDataFrame(
    gps_df,
    geometry=gpd.points_from_xy(gps_df.longitude_anonymous, gps_df.latitude_anonymous),
    crs="EPSG:4326"
)



# ---- 1. GeoJSONファイルの読み込み ----
# ファイルパスは必要に応じて変更してください
gdf_bus_route = gpd.read_file(gis_file)
if gdf_bus_route.crs is None:
    gdf_bus_route = gdf_bus_route.set_crs("EPSG:4326")
# ---- 3. バスルートとバス停に分割 ----
bus_routes = gdf_bus_route[gdf_bus_route.geometry.geom_type.isin(["LineString", "MultiLineString"])]
bus_stops = gdf_bus_route[gdf_bus_route.geometry.geom_type == "Point"]

# %%

# ---- 4. プロット ----
fig, ax = plt.subplots(figsize=(12, 12))

# バスルート
bus_routes.plot(ax=ax, color="blue", linewidth=1, label="Bus Routes")

# バス停
bus_stops.plot(ax=ax, color="blue", markersize=8, label="Bus Stops")


# 凡例・タイトルなど
ax.set_title(f"{place_name} Bus Layers", fontsize=16)
ax.legend()
ax.set_axis_off()  # 軸を消すと見やすくなります

plt.tight_layout()
plt.savefig(f"{OUT_DIR}/{place}_GIS_layer_bus.png")


# バスルート/バス停のバッファ（投影座標系）
bus_routes_proj = bus_routes.to_crs(projected_crs)
bus_stops_proj = bus_stops.to_crs(projected_crs)
buffered_bus_routes_proj = bus_routes_proj.buffer(30)
buffered_bus_stops_proj = bus_stops_proj.buffer(10)

bus_route_geoms = list(buffered_bus_routes_proj) if len(buffered_bus_routes_proj) > 0 else []
bus_stop_geoms = list(buffered_bus_stops_proj) if len(buffered_bus_stops_proj) > 0 else []
bus_routes_buffer_union_proj = unary_union(bus_route_geoms) if len(bus_route_geoms) > 0 else None
bus_stops_buffer_union_proj = unary_union(bus_stop_geoms) if len(bus_stop_geoms) > 0 else None

# GPSポイントも投影座標系へ
gps_gdf_proj = gps_gdf.to_crs(projected_crs)

# ---- 4. hashed_adid + segment_id 単位で集計 ----
results = []
group_cols = ["hashed_adid", "segment_id"]

for (adid, seg_id), trip_points in gps_gdf_proj.groupby(group_cols):
    # 時系列順に並べる
    trip_points_sorted = trip_points.sort_values("datetime")

    # セグメントの時間情報（後工程の厳密結合に使用）
    segment_start = pd.to_datetime(trip_points_sorted["datetime"].min())
    segment_end = pd.to_datetime(trip_points_sorted["datetime"].max())
    segment_date = segment_start.date().isoformat()
    num_points = len(trip_points_sorted)

    # バス停との一致
    if bus_stops_buffer_union_proj is not None:
        in_bus_stop_buffer = trip_points_sorted.geometry.apply(lambda p: bus_stops_buffer_union_proj.covers(p))
    else:
        in_bus_stop_buffer = pd.Series([False] * len(trip_points_sorted), index=trip_points_sorted.index)

    # バスルートとの一致
    if bus_routes_buffer_union_proj is not None:
        in_bus_route_buffer = trip_points_sorted.geometry.apply(lambda p: bus_routes_buffer_union_proj.covers(p))
    else:
        in_bus_route_buffer = pd.Series([False] * len(trip_points_sorted), index=trip_points_sorted.index)

    # 結果を保存
    results.append({
    	"hashed_adid": adid,
    	"segment_id": seg_id,
    	"point_ratio_in_bus_route_buffer": in_bus_route_buffer.mean(),  # バスルート近接割合
    	"point_ratio_in_bus_stop_buffer": in_bus_stop_buffer.mean(),  # バス停近接割合
    	"segment_start": segment_start,
    	"segment_end": segment_end,
    	"segment_date": segment_date,
    	"num_points": num_points,

    })

# ---- 5. 結果のDataFrame化 ----
trip_results_df = pd.DataFrame(results)

trip_results_df.to_csv(f"{OUT_PROCESSED}/{month}_trip_results_bus.csv", index=False)