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
def extract_stays_fast(df, roam_dist, stay_dur):
    df = df.reset_index(drop=True)
    stays = []
    i = 0
    while i < len(df):
        # j を10分先までスキップ
        t_start = df.loc[i, 'datetime'] + pd.Timedelta(seconds=stay_dur)
        j = df['datetime'].searchsorted(t_start, side='left')
        if j >= len(df):
            break

        coords = list(zip(df.loc[i:j, 'latitude'], df.loc[i:j, 'longitude']))
        if compute_diameter(coords) > roam_dist:
            i += 1
            continue

        # j をそのまま進めて、最大距離が roam_dist を超えるまで
        while j < len(df):
            coords = list(zip(df.loc[i:j, 'latitude'], df.loc[i:j, 'longitude']))
            if compute_diameter(coords) > roam_dist:
                break
            j += 1

        stay_df = df.loc[i:j-1]
        lat_mean = stay_df['latitude'].mean()
        lon_mean = stay_df['longitude'].mean()
        # 近似Medoid: 中心に最も近い点
        dists = ((stay_df['latitude'] - lat_mean)**2 + (stay_df['longitude'] - lon_mean)**2)
        medoid_index = dists.idxmin()
        medoid_point = df.loc[medoid_index]

        stays.append({
            'hashed_adid': df.loc[i, 'hashed_adid'],
            'start_time': df.loc[i, 'datetime'],
            'end_time': df.loc[j-1, 'datetime'],
            'latitude': medoid_point['latitude'],
            'longitude': medoid_point['longitude']
        })

        i = j  # ステイの終点の次から開始
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
                    ).drop(columns=["uuid", 'mesh', 'os', 'time_diff_min', 'lat_prev', "lon_prev"])
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