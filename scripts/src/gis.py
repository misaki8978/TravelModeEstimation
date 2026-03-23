import os
import sys
import pandas as pd
import numpy as np
import geopandas as gpd
import osmnx as ox
import matplotlib.pyplot as plt
import shapefile
from shapely.geometry import Point
from shapely.ops import unary_union
from pyproj import Geod
import pandas as pd
import numpy as np

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/log_09_gis.txt"

# 欠損した SHX を読み込み時に復元する（GDAL/OGR 設定）
os.environ.setdefault("SHAPE_RESTORE_SHX", "YES")

# ファイルを2つに分割して、バスのデータフレームとセグメントのgeopandasデータフレームに変換
def division_several_file(files):
    split_idx = files.index('--')
    # log_message(f"{split_idx}", log_path)
    file, gis_file, rail_file, bus_stop_file, train_stop_file = files[:split_idx], files[split_idx + 1], files[-3], files[-2], files[-1]
    path_parts = file[0].split("/")
    # log_message(f"{path_parts}", log_path)
    place = '_'.join(path_parts[-3].split('_')[-2:])
    year = path_parts[-2]
    bus_df = gpd.read_file(gis_file, encoding='cp932', config_options={"SHAPE_RESTORE_SHX": "YES"})
    
    bus_gdf = bus_df.set_crs(epsg=4326)
    bus_gdf = bus_gdf.to_crs(epsg=32652)

    rail_df = gpd.read_file(rail_file, encoding='cp932', config_options={"SHAPE_RESTORE_SHX": "YES"})
    rail_gdf = rail_df.set_crs(epsg=4326)
    rail_gdf = rail_gdf.to_crs(epsg=32652)

    bus_stop_df = gpd.read_file(bus_stop_file, encoding='cp932', config_options={"SHAPE_RESTORE_SHX": "YES"})
    bus_stop_gdf = bus_stop_df.set_crs(epsg=4326)
    bus_stop_gdf = bus_stop_gdf.to_crs(epsg=32652)

    train_stop_df = gpd.read_file(train_stop_file, encoding='cp932', config_options={"SHAPE_RESTORE_SHX": "YES"})
    train_stop_gdf = train_stop_df.set_crs(epsg=4326)
    train_stop_gdf = train_stop_gdf.to_crs(epsg=32652)

    # log_message(f"{bus_gdf.head()}", log_path)
    return bus_gdf, rail_gdf, bus_stop_gdf, train_stop_gdf, file, year, place

#2つの関連のないファイルを受け取ったとき
def division_2file(files):
    split_idx = files.index('--')
    # log_message(f"{split_idx}", log_path)
    file, gis_file = files[split_idx-1], files[split_idx + 1]
    path_parts = file.split("/")
    # log_message(f"{path_parts}", log_path)
    place = '_'.join(path_parts[-3].split('_')[-2:])
    year = path_parts[-2]
    bus_df = gpd.read_file(gis_file, config_options={"SHAPE_RESTORE_SHX": "YES"})
    # bus_routes = bus_df[bus_df.geometry.geom_type.isin(["LineString", "MultiLineString"])]
    # bus_stops = bus_df[bus_df.geometry.geom_type == "Point"]
    bus_gdf = bus_df.set_crs(epsg=4326)
    bus_gdf = bus_gdf.to_crs(epsg=32652)
    gps_file = pd.read_csv(file, parse_dates=['datetime'], compression="gzip")
    gps_df = gps_file.assign(rail_frag = np.where(gps_file["train_rate"] == 1.0, 1, 0),
                            bus_frag = np.where(gps_file["bus_rate"] == 1.0, 1, 0))
    gps_gdf = gpd.GeoDataFrame(
        gps_df,
        geometry=gpd.points_from_xy(gps_file["longitude_anonymous"], gps_file["latitude_anonymous"]),
        crs="EPSG:4326"
    )
    gps_gdf = gps_gdf.to_crs(epsg=32652)
    log_message(f"{gps_gdf["rail_frag"].value_counts()}", log_path)
    log_message(f"{gps_gdf["bus_frag"].value_counts()}", log_path)
    return bus_gdf, gps_gdf, year, place



#バッファーを作成
def make_buffer(gdf, buffer_distance):
    # log_message(f"{buffer_distance}", log_path)
    buffer_gdf = gdf.buffer(buffer_distance)
    buffer_list = list(buffer_gdf)
    buffer_union = unary_union(buffer_list)
    return buffer_union

#バッファー内にあるかどうかを確認
def is_in_buffer(gdf, buffer):
    # log_message(f"{gdf.crs}", log_path)
    # log_message(f"{buffer}", log_path)
    # log_message(f"gdf:{gdf.total_bounds}, buffer:{buffer.bounds}", log_path)
    func = lambda bool_: 1 if bool_ else 0
    return gdf.intersects(buffer).apply(func)

def is_in_buffer_series(points_gdf, buffer_geom):
    # 事前に points_gdf と buffer_geom が同じ CRS（例: EPSG:3857）であることを保証
    return points_gdf.geometry.apply(buffer_geom.covers)

def file_to_gdf(file):
    month = "_".join(file.split('/')[-1].split('_')[:2])
    # month = file.split('/')[-1].split('_')[0]

    df = pd.read_csv(file, parse_dates=['datetime'], compression="gzip")\
                    .assign(
                        segment_month_id = lambda x: month + "_" + x['segment_id'].astype(str)
                    )
    # log_message(f"{df["label"].value_counts()}", log_path)
    # segment_df = df.query("label == 'non-walk'")
    # walk_df = df.query("label == 'walk'")
    segment_df = df.query("is_walk == 0")
    walk_df = df.query("is_walk == 1")
    # log_message(f"{len(walk_df)} walk GPS", log_path)
    # log_message(f"{segment_df["longitude_anonymous"].min()} {segment_df["longitude_anonymous"].max()} {segment_df["latitude_anonymous"].min()} {segment_df["latitude_anonymous"].max()}", log_path)
    segment_gdf = gpd.GeoDataFrame(segment_df, 
                                   geometry=gpd.points_from_xy(segment_df['longitude_anonymous'], segment_df['latitude_anonymous']),
                                #    crs="EPSG:32652"
                                   crs="EPSG:4326"
                                   )
    walk_gdf = gpd.GeoDataFrame(walk_df, 
                                   geometry=gpd.points_from_xy(walk_df['longitude_anonymous'], walk_df['latitude_anonymous']),
                                #    crs="EPSG:32652"
                                   crs="EPSG:4326"
                                   )
    walk_gdf = walk_gdf.to_crs(epsg=32652)
    segment_gdf = segment_gdf.to_crs(epsg=32652)
    # log_message(f"{len(walk_gdf)} walk GPS", log_path)
    # log_message(f"{len(segment_gdf)} segment GPS", log_path)

    return segment_gdf, walk_gdf, month


# --- 追加: 安全チェック関数（そのまま貼り付けOK） ---
def _ensure_same_crs(points_gdf, buffer_geom, *, target_crs):
    # buffer_geom は Shapely なので crsは持てません。target_crs に揃えるのがコツ。
    if points_gdf.crs is None:
        raise ValueError("points_gdf.crs is None。set_crs(EPSG:4326) → to_crs(target) の順で与えてください。")
    if str(points_gdf.crs).lower() != str(target_crs).lower():
        points_gdf = points_gdf.to_crs(target_crs)
    return points_gdf

def _covers_flag(points_gdf, buffer_geom):
    # covers: 境界上も内側扱い（点がバッファ境界に乗っても1）
    # GeoSeries APIで一括判定（スカラーPolygon/MultiPolygonを渡せる）
    mask = points_gdf.geometry.covered_by(buffer_geom)   # True/False の Series
    # 空ジオメトリがあれば自動で False になる（仕様どおり）
    return mask.astype(np.int64)
    # return points_gdf.geometry.apply(lambda geom: int(buffer_geom.covers(geom)))

def make_features(gdf, bus_stops_gdf, train_stops_gdf, *, threshold_train=1.0, threshold_bus=1.0):
    group_cols = ['hashed_adid', 'segment_month_id']

    gdf = gdf.to_crs(epsg=4326)

    gdf_enriched = gdf.assign(
                            longitude = lambda x: np.radians(x["longitude_anonymous"]),
                            latitude = lambda x: np.radians(x["latitude_anonymous"]),
                        )\
                    .assign(
                        lat_prev = lambda x: x.groupby(group_cols)["latitude"].shift(1),
                        lon_prev = lambda x: x.groupby(group_cols)["longitude"].shift(1),
                    )\
                    .assign(
                        dist_from_prev = lambda x: getDistanceOfPoints(x['lat_prev'], x['lon_prev'], x['latitude'], x['longitude']),
                        time_diff = lambda x: x.groupby(group_cols)["datetime"].diff().dt.total_seconds(),
                        speed_mps = lambda x: x['dist_from_prev'] / x['time_diff'],
                        acceleration_mps2 = lambda x: x.groupby(group_cols)["speed_mps"].diff() / x['time_diff'],
                    )
    gdf_bcr = gdf_enriched.assign(
            bearing = lambda x: x.apply(
                lambda row: calculate_bearing(
                    row["lat_prev"],
                    row["lon_prev"],
                    row["latitude"],
                    row["longitude"],
                ),
                axis=1
            )
        )
    # 0秒区間は速度・加速度をNaNに
    gdf_enriched.loc[gdf_enriched["time_diff"] == 0, ["speed", "speed_mps", "acceleration_mps2"]] = np.nan

    # bearing change rate は別途グループ単位で算出
    bcr = (
            gdf_bcr
            .groupby(group_cols, group_keys=False)
            .apply(_segment_mbcr)
            .reset_index(name='bcr')
        )
     # ===== stop rate（有効区間に対する割合）=====
    # 論文定義: 区間速度 <= 0.6 m/s の区間数 / (n-1)
    stop_df = (
        gdf_enriched
        .groupby(group_cols)
        .agg(
            valid_intervals=('speed_mps', lambda s: s.notna().sum()),
            n_stops=('speed_mps', lambda s: np.sum(s <= 0.6)),
        )
        .assign(
            stop_rate=lambda x: np.where(x['valid_intervals'] > 0,
                                         x['n_stops'] / x['valid_intervals'],
                                         np.nan)
        )
        .drop(columns=['valid_intervals', 'n_stops'])
        .reset_index()
    )

    bus_stop_df = stop_proximity_rate(gdf_enriched, bus_stops_gdf, 80, 'bus_stop_proximity_rate')
    train_stop_df = stop_proximity_rate(gdf_enriched, train_stops_gdf, 500, 'train_stop_proximity_rate')

    gdf_features = gdf_enriched.groupby(group_cols)\
                        .agg(
                            # label           = ('label', 'first'),
                            is_walk         = ('is_walk', 'first'),
                            move_id         = ('move_id', 'first'),
                            n_points        = ('datetime', 'count'),
                            all_distance    = ('dist_from_prev', 'sum'),
                            all_time        = ('time_diff', 'sum'),
                            date           = ('datetime', 'min'),
                            date_max           = ('datetime', 'max'),
                            mean_speed  = ('speed_mps', 'mean'),
                            max_speed   = ('speed_mps', 'max'),
                            min_speed   = ('speed_mps', 'min'),
                            mean_accel = ('acceleration_mps2', 'mean'),
                            max_accel  = ('acceleration_mps2', 'max'),
                            train_in_buffer = ('train_in_buffer', 'sum'),
                            bus_in_buffer = ('bus_in_buffer', 'sum'),
                        )\
                        .reset_index()\
                        .merge(bcr, on=group_cols, how='left')\
                        .merge(stop_df, on=group_cols, how='left')\
                        .merge(bus_stop_df, on=group_cols, how='left')\
                        .merge(train_stop_df, on=group_cols, how='left')\
                        .assign(
                            bearing_change_rate = lambda x: x['bcr']/x["all_time"],
                            buffer_train = lambda x: (x['train_in_buffer'] / x['n_points']).fillna(0),
                            buffer_bus = lambda x: (x['bus_in_buffer'] / x['n_points']).fillna(0),
                            rail_flag = lambda x: x['buffer_train'].fillna(0).ge(threshold_train).astype('int8'),
                            bus_flag = lambda x: x['buffer_bus'].fillna(0).ge(threshold_bus).astype('int8'),
                        ).drop(columns=['train_in_buffer', 'bus_in_buffer'])
    return gdf_features

def make_features_walk(gdf):
    group_cols = ['hashed_adid', 'segment_month_id']

    gdf = gdf.to_crs(epsg=4326)

    gdf_enriched = gdf.assign(
                            longitude = lambda x: np.radians(x["longitude_anonymous"]),
                            latitude = lambda x: np.radians(x["latitude_anonymous"]),
                        )\
                    .assign(
                        lat_prev = lambda x: x.groupby(group_cols)["latitude"].shift(1),
                        lon_prev = lambda x: x.groupby(group_cols)["longitude"].shift(1),
                    )\
                    .assign(
                        dist_from_prev = lambda x: getDistanceOfPoints(x['lat_prev'], x['lon_prev'], x['latitude'], x['longitude']),
                        time_diff = lambda x: x.groupby(group_cols)["datetime"].diff().dt.total_seconds(),
                        speed_mps = lambda x: x['dist_from_prev'] / x['time_diff'],
                        acceleration_mps2 = lambda x: x.groupby(group_cols)["speed_mps"].diff() / x['time_diff'],
                    )
    # 0秒区間は速度・加速度をNaNに
    gdf_enriched.loc[gdf_enriched["time_diff"] == 0, ["speed", "speed_mps", "acceleration_mps2"]] = np.nan

    # bearing change rate は別途グループ単位で算出
    # bcr = (
    #     gdf_enriched
    #         .sort_values('datetime')
    #         .groupby(group_cols)
    #         .apply(lambda df: bearing_change_rate_vectorized(
    #             df['bearing'].dropna(),
    #             df.loc[df['bearing'].notna(), 'datetime']
    #         ))
    #         .reset_index(name='bearing_change_rate')
    # )

    gdf_features = gdf_enriched.groupby(group_cols)\
                        .agg(
                            # label           = ('label', 'first'),
                            is_walk         = ('is_walk', 'first'),
                            move_id         = ('move_id', 'first'),
                            n_points        = ('datetime', 'count'),
                            date           = ('datetime', 'min'),
                            date_max           = ('datetime', 'max'),
                            all_distance    = ('dist_from_prev', 'sum'),
                            all_time        = ('time_diff', 'sum'),
                            mean_speed  = ('speed_mps', 'mean'),
                            max_speed   = ('speed_mps', 'max'),
                            min_speed   = ('speed_mps', 'min'),
                            mean_accel = ('acceleration_mps2', 'mean'),
                            max_accel  = ('acceleration_mps2', 'max')
                        )\
                        .reset_index()\
                        .assign(
                            bus_stop_proximity_rate = "NaN",
                            train_stop_proximity_rate = "NaN",
                            bearing_change_rate = "NaN",
                            stop_rate = "NaN",
                            buffer_train = "NaN",
                            buffer_bus = "NaN",
                            rail_flag = "NaN",
                            bus_flag = "NaN",
                        )
    return gdf_features

# def calculate_bearing(lat1, lon1, lat2, lon2):
#     """
#     2点間の方位角（度）を計算
#     """
#     # lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
#     dlon = lon2 - lon1
#     x = np.sin(dlon) * np.cos(lat2)
#     y = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
#     theta = np.arctan2(x, y)
#     return (np.degrees(theta) + 360) % 360  # 0〜360に正規化


def bearing_change_rate_vectorized(bearings, times):
    """
    bearing列とdatetime列から区間全体のbearing change rateを計算
    """
    # Series/ndarray の両方を受け取り、NaN を除去
    bearings = np.asarray(bearings, dtype=float)
    bearings = bearings[~np.isnan(bearings)]
    if bearings.size < 2:
        return np.nan

    # bearing差分 (-180~180に正規化)
    diffs = np.diff(bearings)
    diffs = np.where(diffs > 180, diffs - 360, diffs)
    diffs = np.where(diffs < -180, diffs + 360, diffs)

    # total_change = np.sum(diffs)
    total_change = np.sum(np.abs(diffs))  # 絶対値で合計

    # duration (秒)
    # duration = (times.iloc[-1] - times.iloc[0]).total_seconds()
    dt = (times[1:].astype("datetime64[ns]") - times[:-1].astype("datetime64[ns]")) / np.timedelta64(1, "s")
    duration = float(np.nansum(np.clip(dt, a_min=0, a_max=None)))
    if duration == 0:
        return np.nan

    return total_change / duration  # deg/sec

def getDistanceOfPoints(lat1, lon1, lat2, lon2):
    # lat1 = np.radians(lat1.astype(float))
    # lon1 = np.radians(lon1.astype(float))
    # lat2 = np.radians(lat2.astype(float))
    # lon2 = np.radians(lon2.astype(float))
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2.0 * np.arcsin(np.sqrt(a))
    m = 6371000 * c
    return m

def plot_gis_layer(gdf, gis_file, gdf_pref, mode, out_dir, color):
    fig, ax = plt.subplots(figsize=(10, 10))
    gdf_pref.query("prefecture == '大阪府'", engine='python').plot(ax=ax, color='gray')
    target_gdf = gdf.query(f"{mode}_frag == 1", engine='python')
    gis_file.plot(ax=ax, color=color[f"{mode}_layer"], linewidth=1, label=mode)
    target_gdf.plot(ax=ax, color=color[mode], markersize=2, alpha=0.7)
    log_message(f"{len(target_gdf)} {mode} like GPS", log_path)
    ax.set_title(f"{mode} Route and {mode} like GPS")
    ax.set_axis_off()
    plt.tight_layout()
    plt.legend()
    plt.savefig(f"{out_dir}/{mode}_route_and_like_gps.png")
    plt.close(fig)


def plot_gpspoint(df, pref_gdf, out_dir):
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["longitude_anonymous"], df["latitude_anonymous"]),
        crs="EPSG:4326",
    )
    fig, ax = plt.subplots(figsize=(10, 10))
    pref_gdf.plot(ax=ax, color='gray')
    
    gdf.plot(ax=ax, color='skyblue', markersize=0.4, alpha=0.7)
    # gis_file.plot(ax=ax, color='blue', linewidth=1)
    # log_message(f"{len(gdf)} GPS", log_path)
    ax.set_title(f"GPS")
    ax.set_axis_off()
    # plt.tight_layout()
    plt.legend()
    plt.savefig(f"{out_dir}/gps.png")
    plt.close(fig)

def _geod_distance_m(
    lon1: float, lat1: float, lon2: float, lat2: float
) -> float:
    """2点間の測地線距離（メートル）を返す。どれかNaNならNaN。"""

    WGS84 = Geod(ellps="WGS84")
    if pd.isna(lon1) or pd.isna(lat1) or pd.isna(lon2) or pd.isna(lat2):
        return np.nan
    # WGS84.invは(方位1→2, 方位2→1, 距離m)を返す
    _, _, dist_m = WGS84.inv(lon1, lat1, lon2, lat2)
    return dist_m

def calculate_bearing(lat1, lon1, lat2, lon2):
    """
    2点間の方位角（bearing）を計算する
    緯度・経度はラジアン単位で入力
    """
    dlon = lon2 - lon1
    x = np.sin(dlon) * np.cos(lat2)
    y = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(dlon)
    bearing = np.arctan2(x, y)
    bearing_deg = np.degrees(bearing)
    return (bearing_deg + 360) % 360  # 0–360°に正規化

def _segment_mbcr(df):
    # 時系列順に
    bearings = df.sort_values('datetime')["bearing"].to_numpy()

    # NaN除去（先頭はNaNになりがち）
    bearings = bearings[~np.isnan(bearings)]
    n_points = len(df) -1  # 「ポイント数」で割る（ご指定どおり）

    # 有効なbearingが2未満 or ポイント数0 の場合は NaN
    if n_points == 0 or len(bearings) < 2:
        return np.nan

    # 隣接差の絶対値（度）
    diffs = np.abs(np.diff(bearings))
    # 最小角度差に正規化（>180 は 360-差）
    diffs = np.where(diffs > 180.0, 360.0 - diffs, diffs)

    # 合計をポイント数で割る
    return float(diffs.sum())



def stop_proximity_rate(gdf_enriched,  stops_gdf, distance_threshold, proximity_col_name):
    # 距離計算のためメートル単位の投影座標系に変換
    # （世界共通でそこそこ使える Web Mercator）
    gdf_low = gdf_enriched[gdf_enriched["speed_mps"] <= 1.0].copy()
    
    gdf_low_geo = gdf_low.set_geometry(gdf_low.geometry).to_crs(epsg=3857)
    stops_3857 = stops_gdf.to_crs(epsg=3857)[["geometry"]]

        # 最近傍 POI との距離を計算
    low_with_nearest = gpd.sjoin_nearest(
        gdf_low_geo,
        stops_3857,
        how="left",
        distance_col="dist_to_poi"
    )

    # --- セグメント単位で集計 ---
    agg_df = (
        low_with_nearest
        .groupby(["hashed_adid", "segment_month_id"])
        .agg(
            n_low_speed=("dist_to_poi", "size"),
            n_near_poi=("dist_to_poi", lambda s: ((s.notna()) & (s <= distance_threshold)).sum()),
        )
        .assign(
            **{
                proximity_col_name: lambda x: (
                    x["n_near_poi"]
                    .div(x["n_low_speed"])
                    .where((x["n_low_speed"] > 0) & (x["n_near_poi"] > 0), -1.0)
                    .astype(float)
                )
            }
        )
        .drop(columns=["n_low_speed", "n_near_poi"])
        .reset_index()
    )


    return agg_df