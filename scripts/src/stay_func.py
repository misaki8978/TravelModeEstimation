import pandas as pd
import sys
import numpy as np
from sklearn.cluster import DBSCAN
from scipy.spatial.distance import pdist
from math import radians, sin, cos, sqrt, atan2, asin

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_04_stay.txt"

# 距離計算
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000.0
    # lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def getDistanceOfPoints(lat1, lon1, lat2, lon2):
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    m = 6371000 * c
    return m
# 直径計算
def compute_diameter(coords):
    # coordsは[[lat1, lon1], [lat2, lon2], ...]の形式
    return np.max(pdist(coords, lambda u, v: haversine_distance(u[0], u[1], v[0], v[1])))

# stay point抽出
# def extract_stays_fast(df, roam_dist, stay_dur):
#     df = df.reset_index(drop=True)
#     stays = []
#     i = 0
#     while i < len(df):
#         # j を10分先までスキップ
#         t_start = df.loc[i, 'datetime'] + pd.Timedelta(seconds=stay_dur)
#         j = df['datetime'].searchsorted(t_start, side='left')
#         if j >= len(df):
#             break

#         coords = list(zip(df.loc[i:j, 'latitude'], df.loc[i:j, 'longitude']))
#         if compute_diameter(coords) > roam_dist:
#             i += 1
#             continue

#         # j をそのまま進めて、最大距離が roam_dist を超えるまで
#         while j < len(df):
#             coords = list(zip(df.loc[i:j, 'latitude'], df.loc[i:j, 'longitude']))
#             if compute_diameter(coords) > roam_dist:
#                 break
#             j += 1

#         stay_df = df.loc[i:j-1]
#         lat_mean = stay_df['latitude'].mean()
#         lon_mean = stay_df['longitude'].mean()
#         # 近似Medoid: 中心に最も近い点
#         dists = ((stay_df['latitude'] - lat_mean)**2 + (stay_df['longitude'] - lon_mean)**2)
#         medoid_index = dists.idxmin()
#         medoid_point = df.loc[medoid_index]

#         stays.append({
#             'hashed_adid': df.loc[i, 'hashed_adid'],
#             'week_start': df.loc[i, 'week_start'],
#             'start_time': df.loc[i, 'datetime'],
#             'end_time': df.loc[j-1, 'datetime'],
#             'latitude': medoid_point['latitude'],
#             'longitude': medoid_point['longitude']
#         })

#         i = j  # ステイの終点の次から開始
#     return pd.DataFrame(stays)

import numpy as np
import pandas as pd
from typing import Tuple, List, Dict, Optional

# ------------------------------
# 距離ユーティリティ（ラジアン前提）
# ------------------------------
EARTH_R = 6371000.0  # meters

def haversine_array(lat1: np.ndarray, lon1: np.ndarray, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    """
    Vectorized haversine distance [meters].
    All inputs are radians.
    """
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat * 0.5) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon * 0.5) ** 2
    return 2.0 * EARTH_R * np.arcsin(np.sqrt(a))

def pairwise_diameter_haversine(lat: np.ndarray, lon: np.ndarray) -> float:
    """
    厳密な直径（最大対間距離）を O(n^2) で計算。呼び出し頻度は境界判定で絞る。
    入力はラジアン。
    """
    n = lat.size
    if n <= 1:
        return 0.0
    # 分割してベクトル化（メモリ節約）
    dmax = 0.0
    for i in range(n - 1):
        d = haversine_array(
            np.full(n - i - 1, lat[i]),
            np.full(n - i - 1, lon[i]),
            lat[i + 1:],
            lon[i + 1:]
        )
        dm = float(d.max())
        if dm > dmax:
            dmax = dm
    return dmax

# ------------------------------
# バウンド（上限・下限）で高速判定
# ------------------------------
def diameter_bounds(
    lat: np.ndarray, lon: np.ndarray, lat_mean: Optional[float] = None, lon_mean: Optional[float] = None
) -> Tuple[float, float]:
    """
    直径の下限・上限境界を返す（[lower_bound, upper_bound]）[meters]
    - 上限：バウンディングボックス対角線距離
    - 下限：重心（平均）からの最大距離×2（真の直径の下限になる）
    入力はラジアン。
    """
    # bbox upper bound
    lat_min, lat_max = lat.min(), lat.max()
    lon_min, lon_max = lon.min(), lon.max()
    ub = float(haversine_array(
        np.array([lat_min]), np.array([lon_min]),
        np.array([lat_max]), np.array([lon_max])
    )[0])

    # center lower bound
    if lat_mean is None:
        lat_mean = float(lat.mean())
    if lon_mean is None:
        lon_mean = float(lon.mean())
    # max distance from centroid
    d_to_center = haversine_array(lat, lon, np.full(lat.size, lat_mean), np.full(lon.size, lon_mean))
    lb = float(d_to_center.max() * 2.0)
    return lb, ub

# ------------------------------
# 二分探索で最大 j を一発で見つける
# ------------------------------
def max_j_within_diameter(
    lat_rad: np.ndarray, lon_rad: np.ndarray, start_idx: int, j_min: int, roam_dist: float
) -> int:
    """
    [start_idx, j) の窓が直径 <= roam_dist となる最大の j を返す。
    まず指数的拡張で広げ、境界で二分探索。
    入力配列はラジアン。
    """
    n = lat_rad.size
    # すでに j_min は「最短滞在成立チェック後」の開始候補
    lo = j_min  # 常に条件を満たすとは限らない
    hi = j_min

    # 1) 指数的拡張
    step = 1
    while True:
        cand = hi + step
        if cand > n:
            cand = n
        lat_win = lat_rad[start_idx:cand]
        lon_win = lon_rad[start_idx:cand]
        # 軽量判定：境界でふるい落とし
        lb, ub = diameter_bounds(lat_win, lon_win)
        if lb > roam_dist:
            # これ以上伸ばせない
            break
        if ub <= roam_dist:
            # まだ伸ばせる
            hi = cand
            if cand == n:
                # もう末尾までOK
                return n
            step *= 2
            continue
        # 不確定域 → 厳密直径で判定
        exact = pairwise_diameter_haversine(lat_win, lon_win)
        if exact <= roam_dist:
            hi = cand
            if cand == n:
                return n
            step *= 2
            continue
        else:
            break  # cand は NG、hi は OK の上限候補

    # 2) 二分探索： (hi, cand) の間で最大 OK を探す
    # 現在: hi は OK か未確定、cand は NG（または n）
    lo_ok = hi  # 現時点での最大OK
    ng = min(n, hi + step)  # 最初の NG 候補
    while lo_ok + 1 < ng:
        mid = (lo_ok + ng) // 2
        lat_win = lat_rad[start_idx:mid]
        lon_win = lon_rad[start_idx:mid]
        lb, ub = diameter_bounds(lat_win, lon_win)
        if lb > roam_dist:
            ng = mid
            continue
        if ub <= roam_dist:
            lo_ok = mid
            continue
        exact = pairwise_diameter_haversine(lat_win, lon_win)
        if exact <= roam_dist:
            lo_ok = mid
        else:
            ng = mid
    return lo_ok

# ------------------------------
# メイン：高速 stay 抽出
# ------------------------------
def extract_stays_fast(
    df: pd.DataFrame, roam_dist: float, stay_dur: float
) -> pd.DataFrame:
    """
    高速化版のステイ抽出。
    - 直径の上限/下限境界で早期判定
    - 指数的拡張 + 二分探索で最大 j を発見
    - ベクトル化＆タプル生成回避
    前提: df は単一ユーザ・単一週などのまとまりで、datetime昇順。
    必要列: ['hashed_adid','week_start','datetime','latitude','longitude']
    """
    if df.empty:
        return pd.DataFrame(columns=[
            'hashed_adid','week_start','start_time','end_time','latitude','longitude'
        ])

    # インデックス・型の整備
    df = df.sort_values('datetime').reset_index(drop=True)
    lat_rad = np.radians(df['latitude'].to_numpy(dtype=np.float64))
    lon_rad = np.radians(df['longitude'].to_numpy(dtype=np.float64))
    n = len(df)

    stays: List[Dict] = []
    i = 0
    while i < n:
        # 滞在最短時間の開始候補 j を一発検索
        t_start = df.at[i, 'datetime'] + pd.Timedelta(seconds=stay_dur)
        j0 = int(df['datetime'].searchsorted(t_start, side='left'))
        if j0 >= n:
            break

        # まず最短窓 [i, j0) が roam_dist を満たすかを軽量判定
        lat_win0 = lat_rad[i:j0]
        lon_win0 = lon_rad[i:j0]
        lb0, ub0 = diameter_bounds(lat_win0, lon_win0)
        if lb0 > roam_dist:
            i += 1
            continue
        if ub0 > roam_dist:
            # ここだけ厳密直径
            if pairwise_diameter_haversine(lat_win0, lon_win0) > roam_dist:
                i += 1
                continue

        # OK なら最大 j を指数拡張＋二分探索で取得
        j = max_j_within_diameter(lat_rad, lon_rad, i, j0, roam_dist)
        # 滞在セグメント [i, j)
        end_idx = j - 1
        stay_slice = slice(i, j)  # Python slice でも pandas の再インデックス回避
        # medoid（近似：重心に最も近い点）
        lat_seg = df['latitude'].to_numpy()[stay_slice]
        lon_seg = df['longitude'].to_numpy()[stay_slice]
        lat_mean = lat_seg.mean()
        lon_mean = lon_seg.mean()
        # ユークリッド近似（ラジアンに変換せずOK：中央値選びの相対距離なので十分に高速）
        d2 = (lat_seg - lat_mean) ** 2 + (lon_seg - lon_mean) ** 2
        medoid_local = int(np.argmin(d2)) + i
        med = df.iloc[medoid_local]

        stays.append({
            'hashed_adid': df.at[i, 'hashed_adid'],
            'week_start': df.at[i, 'week_start'],
            'start_time': df.at[i, 'datetime'],
            'end_time': df.at[end_idx, 'datetime'],
            'latitude': med['latitude'],
            'longitude': med['longitude'],
        })

        i = j  # 次へ

    return pd.DataFrame(stays)



#moveごとにidを振る
def stay_to_move(df_with_stay_id):
    # 文字列型のdatetimeをdatetime型に変換 
    gap_minutes = 20
    
    # まず移動部分だけ抽出
    return df_with_stay_id.assign(datetime=lambda x: pd.to_datetime(x['datetime']))\
                          .query("stay_id == -1", engine='python')\
                        .assign(
                            time_diff_min=lambda x: (x['datetime'].diff().dt.total_seconds().div(60).fillna(0)),
                            idx_gap=lambda x: x.index.to_series().diff().ne(1),
                            time_gap=lambda x: (x['datetime'].diff().dt.total_seconds().fillna(0).ge(gap_minutes * 60)),
                            group_change=lambda x: (x['idx_gap'] | x['time_gap']).cumsum(),
                            move_id=lambda x: x['group_change'] - x['group_change'].min()
                        )\
                        .drop(columns=["idx_gap", "time_gap", "group_change"])


# DBSCAN
def run_dbscan_on_stays(stay_df, eps_meters, min_samples):

    if stay_df.empty:
        return stay_df
    # 緯度経度 → ラジアン（DBSCAN の haversine 距離で使うため）
    coords = np.radians(
        stay_df.filter(items=['latitude', 'longitude'])\
        .astype(np.float64)\
        .values
        )
    
    # 地球半径でスケール変換：eps[m] → eps[rad]
    eps_rad = eps_meters / 6371000  # 地球半径: 約6371km

    db = DBSCAN(eps=eps_rad, min_samples=min_samples, metric='haversine')
    labels = db.fit_predict(coords)
    
    stay_df = stay_df.copy()
    stay_df['cluster'] = labels
    # ノイズ（-1）を正の一意なIDに置き換え
    max_cluster = stay_df['cluster'].max()
    next_cluster = max_cluster + 1

    for idx in stay_df[stay_df['cluster'] == -1].index:
        stay_df.at[idx, 'cluster'] = next_cluster
        next_cluster += 1
    return stay_df

# 滞在idを振る
def assign_stay_ids(df, clustered_stays):
    if clustered_stays.empty:
        return df.assign(stay_id=-1)
    
    df['stay_id'] = -1  # デフォルトは -1（移動）
    clustered_stays = clustered_stays.assign(start_time=lambda x: x['datetime'].min(),
                                            end_time=lambda x: x['datetime'].max())
    for _, row in clustered_stays.iterrows():
        mask = (df['datetime'] >= row['start_time']) & (df['datetime'] <= row['end_time'])
        df.loc[mask, 'stay_id'] = row['cluster']
    
    return df

#tripごとのサマリー作成
def func_move_summary(move_df):

    # サマリー統計量の計算
    move_summary = move_df.groupby('move_id', as_index=False).agg({
        'hashed_adid': 'first',
        'datetime': 'count',
        'distance_m': 'sum',
        'P_speed': 'mean',
        'time_diff_s': 'sum'
    }).rename(columns={
        'datetime': 'point_count',
        'distance_m': 'move_total_distance',
        'P_speed': 'P_speed_avg',
        'time_diff_s': 'move_total_duration_sec'
    })
    
    return move_summary

#速度計算  
def speed_calc(move_df):

    move_df = move_df\
                    .assign(datetime=lambda x: pd.to_datetime(x['datetime'])
                            )\
                    .sort_values(['move_id', 'datetime'], ascending=[True, True])
    # 距離と時間差を使って各ポイントの速度 (m/s) を計算
    move_df = move_df.assign(
                        lat_prev=lambda x: x.groupby('move_id')['latitude'].shift(1),
                        lon_prev=lambda x: x.groupby('move_id')['longitude'].shift(1),
                        # time_prev=lambda x: x.groupby('move_id')['datetime'].shift(1),
                        distance_m=lambda x: x.apply(lambda row: getDistanceOfPoints(
                            row['lat_prev'], 
                            row['lon_prev'], 
                            row['latitude'], 
                            row['longitude']
                        ) if pd.notna(row['lat_prev']) else 0, axis=1),
                        time_diff_s=lambda x: x.groupby('move_id')['datetime'].diff().dt.total_seconds(),
                        P_speed=lambda x: np.where(x['time_diff_s'] > 0, x['distance_m'] / x['time_diff_s'], np.nan),
                    ).drop(columns=['time_diff_min', 'lat_prev', "lon_prev"])
    # # ステップ5: 異常値（40 m/s 超）を除外して別の DataFrame に保存
    # filtered_df = move_df[move_df['P_speed'] <= 30]\
    #                 .groupby('move_id')\
    #                 .filter(lambda x: len(x) >= 3)

    # filtered_df = filtered_df[["hashed_adid", "move_id", "datetime", "latitude_anonymous", "longitude_anonymous",  "accuracy"]]\
    #                 .assign(
    #                     lat_prev=lambda x: x.groupby('move_id')['latitude_anonymous'].shift(1),
    #                     lon_prev=lambda x: x.groupby('move_id')['longitude_anonymous'].shift(1),
    #                     time_prev=lambda x: x.groupby('move_id')['datetime'].shift(1),
    #                     distance_m=lambda x: x.apply(lambda row: haversine_distance(
    #                         row['lat_prev'], 
    #                         row['lon_prev'], 
    #                         row['latitude_anonymous'], 
    #                         row['longitude_anonymous']
    #                     ) if pd.notna(row['lat_prev']) else 0, axis=1),
    #                     time_diff_s=lambda x: (x['datetime'] - x['time_prev']).dt.total_seconds(),
    #                     P_speed=lambda x: x['distance_m'] / x['time_diff_s'],
    #                     S_speed_avg=lambda x: x.groupby('move_id')['P_speed'].transform('mean')
    #                 )\
    #                 .drop(columns=["lat_prev", "lon_prev", "time_prev", "time_diff_s"])\
    #                 .reset_index(drop=True)

    return move_df


#滞在判定
def stay_detection(df, roam_dist, stay_dur, eps_meters, min_samples):
    stay_df = extract_stays_fast(df, roam_dist, stay_dur)
    clustered_stays = run_dbscan_on_stays(stay_df, eps_meters, min_samples)
    stay_ad_df = assign_stay_ids(df, clustered_stays)
    move_df = stay_to_move(stay_ad_df)
    speed_GPS = speed_calc(move_df).drop(columns=["latitude", "longitude"])
    move_summary = func_move_summary(speed_GPS)
    return clustered_stays, speed_GPS, move_summary