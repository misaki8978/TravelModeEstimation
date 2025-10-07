import os
import sys
import gzip
import numpy as np
import pandas as pd
from glob import glob
import geopandas as gpd
import plotly.graph_objects as go
from shapely.geometry import Point, Polygon
from shapely.ops import unary_union
import matplotlib.pyplot as plt
import osmnx as ox
from japanmap import get_data, pref_points, pref_names

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

place_name = "Nagasaki, Japan"
# 推定UTMに統一（失敗時は長崎のUTM 52Nにフォールバック）
try:
    projected_crs = gps_gdf.estimate_utm_crs()
    if projected_crs is None:
        projected_crs = "EPSG:32652"
except Exception:
    projected_crs = "EPSG:32652"
# log_message(f"Using projected CRS: {str(projected_crs)}", log_path)


# 鉄道ネットワーク（鉄道 + 地下鉄 + ライトレール）取得
G = ox.graph_from_place(place_name, network_type='all_private',
                        custom_filter='["railway"~"rail|subway|light_rail"]')
nodes, edges = ox.graph_to_gdfs(G)

# 線形の鉄道にバッファを作成（投影座標系に統一）
edges_proj = edges.to_crs(projected_crs)
buffered_edges = edges_proj.buffer(50)
buffered_gdf = gpd.GeoDataFrame(geometry=buffered_edges, crs=edges_proj.crs)


# %%
# 路面電車データ（tram）の取得
tags_tram = {"railway": "tram"}
tram_lines = ox.features_from_place(place_name, tags_tram)
tram_lines = tram_lines[tram_lines.geometry.type == "LineString"]
tram_lines_proj = tram_lines.to_crs(projected_crs)
buffered_tram = tram_lines_proj.buffer(30)
buffered_tram_gdf = gpd.GeoDataFrame(geometry=buffered_tram, crs=tram_lines_proj.crs)

# %%
# 鉄道駅（station）を取得
tags_station = {"railway": "station"}
stations = ox.features_from_place(place_name, tags_station)
stations_points = stations[stations.geometry.type == "Point"]
# %%
# ---- 1. GeoJSONファイルの読み込み ----
# ファイルパスは必要に応じて変更してください
gdf_bus_route = gpd.read_file(gis_file)
# 入力GISのCRSが未設定ならWGS84とみなす（GeoJSON想定）
if gdf_bus_route.crs is None:
    gdf_bus_route = gdf_bus_route.set_crs("EPSG:4326")
# ---- 3. バスルートとバス停に分割 ----
bus_routes = gdf_bus_route[gdf_bus_route.geometry.geom_type.isin(["LineString", "MultiLineString"])]
bus_stops = gdf_bus_route[gdf_bus_route.geometry.geom_type == "Point"]

pref_poly = [Polygon(points) for points in pref_points(get_data())]
gdf_pref = gpd.GeoDataFrame(crs = edges_proj.crs, geometry=pref_poly)
gdf_pref['prefecture'] = pref_names[1:]

# %%

# ---- 4. プロット ----
fig, ax = plt.subplots(figsize=(12, 12))

gdf_pref.query("prefecture == '長崎県'", engine='python').plot(ax=ax, color='gray')

# バスルート
bus_routes.plot(ax=ax, color="skyblue", linewidth=1, label="Bus Routes")

# バス停
bus_stops.plot(ax=ax, color="skyblue", markersize=8, label="Bus Stops")

# 鉄道（Rail）
# buffered_gdf.plot(ax=ax, color='green', alpha=0.4, label='Rail Buffer')
edges.to_crs(epsg=4326).plot(ax=ax, color='red', linewidth=1, label='Rail')

# 駅（Stations）
stations_points.plot(ax=ax, color='red', markersize=15, marker='*', label='Stations')

# 路面電車（Tram）
# buffered_tram_gdf.plot(ax=ax, color='purple', alpha=0.4, label='Tram Buffer')
tram_lines.to_crs(epsg=4326).plot(ax=ax, color='darkviolet', linewidth=1, label='Tram Lines')


# 凡例・タイトルなど
ax.set_title(f"{place_name} Railway and Bus Layers", fontsize=16)
ax.legend()
ax.set_axis_off()  # 軸を消すと見やすくなります

plt.tight_layout()
plt.savefig(f"{OUT_DIR}/{place}_GIS_layer.png")



# ---- 5. 空間演算のためのバッファUnion（投影座標系のまま）----


# 鉄道（rail/subway/light_rail）＋ 路面電車（tram）の統合バッファ（投影座標系）
rail_geoms = list(buffered_edges) if len(buffered_edges) > 0 else []
# if len(buffered_tram) > 0:
#     rail_geoms += list(buffered_tram)
rail_buffer_union_proj = unary_union(rail_geoms) if len(rail_geoms) > 0 else None

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
    trip_points_sorted = trip_points.sort_values(["datetime"], ascending=[True])

    # 鉄道（route=railway）のバッファ一致
    if rail_buffer_union_proj is not None:
        in_rail_buffer = trip_points_sorted.geometry.apply(lambda p: rail_buffer_union_proj.covers(p))
    else:
        in_rail_buffer = pd.Series([False] * len(trip_points_sorted), index=trip_points_sorted.index)

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

    # 始点・終点が鉄道buffer内かどうか
    # セグメントの時間情報（後工程の厳密結合に使用）
    segment_start = pd.to_datetime(trip_points_sorted["datetime"].min())
    segment_end = pd.to_datetime(trip_points_sorted["datetime"].max())
    segment_date = segment_start.date().isoformat()
    num_points = len(trip_points_sorted)
    # start_point = trip_points_sorted.iloc[0].geometry
    # end_point   = trip_points_sorted.iloc[-1].geometry
    # start_in_rail = (rail_buffer_union_proj.covers(start_point) if rail_buffer_union_proj is not None else False)
    # end_in_rail   = (rail_buffer_union_proj.covers(end_point) if rail_buffer_union_proj is not None else False)

    # 結果を保存
    results.append({
    	"hashed_adid": adid,
    	"segment_id": seg_id,
    	"point_ratio_in_rail_buffer": in_rail_buffer.mean(),  # 鉄道/路面電車 近接割合
    	"point_ratio_in_bus_route_buffer": in_bus_route_buffer.mean(),  # バスルート近接割合
    	"point_ratio_in_bus_stop_buffer": in_bus_stop_buffer.mean(),  # バス停近接割合
        "segment_start": segment_start,
    	"segment_end": segment_end,
    	"segment_date": segment_date,
    	"num_points": num_points,
    	# "start_in_rail_buffer": start_in_rail,
    	# "end_in_rail_buffer": end_in_rail,

    })

# ---- 5. 結果のDataFrame化 ----
trip_results_df = pd.DataFrame(results)
log_message(f"{trip_results_df['point_ratio_in_bus_route_buffer'].value_counts()}", log_path)

trip_results_df.to_csv(f"{OUT_PROCESSED}/{month}_trip_results_density.csv", index=False)