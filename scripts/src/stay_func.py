import pandas as pd
import sys
import numpy as np
from sklearn.cluster import DBSCAN
from scipy.spatial.distance import pdist
from math import radians, sin, cos, sqrt, atan2

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

# 直径計算
def compute_diameter(coords):
    # coordsは[[lat1, lon1], [lat2, lon2], ...]の形式
    return np.max(pdist(coords, lambda u, v: haversine_distance(u[0], u[1], v[0], v[1])))

# stay point抽出
def extract_stays_fast(df, roam_dist, stay_dur):
    stays = []
    i = 0
    while i < len(df):
        # j を10分先までスキップ
        t_start = df.loc[i, 'datetime'] + pd.Timedelta(seconds=stay_dur)
        j = df['datetime'].searchsorted(t_start, side='left')
        if j >= len(df):
            break

        coords = list(zip(df.loc[i:j, 'latitude_anonymous'], df.loc[i:j, 'longitude_anonymous']))
        if compute_diameter(coords) > roam_dist:
            i += 1
            continue

        # j をそのまま進めて、最大距離が roam_dist を超えるまで
        while j < len(df):
            coords = list(zip(df.loc[i:j, 'latitude_anonymous'], df.loc[i:j, 'longitude_anonymous']))
            if compute_diameter(coords) > roam_dist:
                break
            j += 1

        stay_df = df.loc[i:j-1]
        lat_mean = stay_df['latitude_anonymous'].mean()
        lon_mean = stay_df['longitude_anonymous'].mean()
        # 近似Medoid: 中心に最も近い点
        dists = ((stay_df['latitude_anonymous'] - lat_mean)**2 + (stay_df['longitude_anonymous'] - lon_mean)**2)
        medoid_index = dists.idxmin()
        medoid_point = df.loc[medoid_index]

        stays.append({
            'start_time': df.loc[i, 'datetime'],
            'end_time': df.loc[j-1, 'datetime'],
            'latitude': medoid_point['latitude_anonymous'],
            'longitude': medoid_point['longitude_anonymous']
        })

        i = j  # ステイの終点の次から開始
    return pd.DataFrame(stays)


#moveごとにidを振る
def stay_to_move(df_with_stay_id):
    gap_minutes = 20
    
    # まず移動部分だけ抽出
    move_df = df_with_stay_id[df_with_stay_id["stay_id"] == -1]\
                .assign(
                    time_diff_min=lambda x: (x['datetime'].diff().dt.total_seconds().div(60).fillna(0)),
                    idx_gap=lambda x: x.index.to_series().diff().ne(1),
                    time_gap=lambda x: (x['datetime'].diff().dt.total_seconds().fillna(0).ge(gap_minutes * 60)),
                    group_change=lambda x: (x['idx_gap'] | x['time_gap']).cumsum(),
                    move_id=lambda x: x['group_change'] - x['group_change'].min()
                )\
                .drop(columns=["idx_gap", "time_gap", "group_change"])

    return move_df

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

    for _, row in clustered_stays.iterrows():
        mask = (df['datetime'] >= row['start_time']) & (df['datetime'] <= row['end_time'])
        df.loc[mask, 'stay_id'] = row['cluster']
    
    return df

#tripごとのサマリー作成
def func_move_summary(move_df):
    # ソート（移動順に並べる）
    move_df = move_df.sort_values(['move_id', 'datetime']).assign(
        lat_next=lambda x: x.groupby('move_id')['latitude_anonymous'].shift(-1),
        lon_next=lambda x: x.groupby('move_id')['longitude_anonymous'].shift(-1)
    ).assign(
        segment_distance=lambda x: x.apply(
            lambda row: haversine_distance(
                row['latitude_anonymous'],
                row['longitude_anonymous'],
                row['lat_next'],
                row['lon_next']
            ) if pd.notna(row['lat_next']) else 0,
            axis=1
        )
    )

    # サマリー統計量の計算
    move_summary = move_df.groupby('move_id', as_index=False).agg({
        'hashed_adid': 'first',
        'latitude_anonymous': 'count',
        'segment_distance': 'sum',
        'P_speed': 'mean',
    }).rename(columns={
        'latitude_anonymous': 'point_count',
        'segment_distance': 'total_distance_m',
        'P_speed': 'P_speed_avg',
        # 'datetime': 'move_duration_sec'
    })
    
    # def calc_move_duration_sec(x):
    #     return (x.max() - x.min()).total_seconds()
    
    # 時間差を別途計算
    duration_sec = move_df.filter(items=['move_id', 'datetime'])\
                          .groupby(by=['move_id'], as_index=False)\
                          .agg(
                              #    lambda x: (x.max() - x.min()).total_seconds()
                              #    {'move_duration_sec': calc_move_duration_sec}
                             move_duration=('datetime', lambda x: x.max() - x.min())
                             )\
                          .merge(move_summary, on='move_id', how='right')\
                          .dropna(subset=['move_duration'], axis='index')\
                          .assign(
                                move_duration_sec=lambda x: x['move_duration'].apply(lambda x: x.total_seconds()).values#[0].astype(np.float64)
                                                            if x['move_duration'].apply(lambda x: x.total_seconds()).size != 0
                                                            else 0,
                                S_speed_avg=lambda x: x['total_distance_m'] / x['move_duration_sec'].astype(np.float64),
                                )
    # log_message(f"duration_sec: {duration_sec.head()}", 
    #             message_path)
                        #   .assign(
                        #           move_duration_sec=lambda x: (x['move_duration'].total_seconds()).values[0],
                        #           S_speed_avg=lambda x: np.where(
                        #             x['total_distance_m'] != 0,
                        #             x['total_distance_m'] / x['move_duration_sec'],
                        #             0)
                        #         )
    # 結合前に dropna で安全な行だけにする
    # duration_sec = duration_sec.dropna().astype(float)

    # move_summary にマージ

    # move_summary = move_summary.join(duration_sec.rename('move_duration_sec'), how='left')

    # move_summary['S_speed_avg'] = move_summary['total_distance_m'] / move_summary['move_duration_sec']
    
    return duration_sec

#速度計算  
def speed_calc(move_df):
    # 距離と時間差を使って各ポイントの速度 (m/s) を計算
    move_df = move_df.sort_values(['move_id'])\
                    .assign(
                        lat_prev=lambda x: x.groupby('move_id')['latitude_anonymous'].shift(-1),
                        lon_prev=lambda x: x.groupby('move_id')['longitude_anonymous'].shift(-1),
                        time_prev=lambda x: x.groupby('move_id')['datetime'].shift(-1),
                        distance_m=lambda x: x.apply(lambda row: haversine_distance(
                            row['lat_prev'], 
                            row['lon_prev'], 
                            row['latitude_anonymous'], 
                            row['longitude_anonymous']
                        ) if pd.notna(row['lat_prev']) else 0, axis=1),
                        time_diff_s=lambda x: (x['datetime'] - x['time_prev']).dt.total_seconds(),
                        P_speed=lambda x: x['distance_m'] / x['time_diff_s'],
                    )

    # ステップ5: 異常値（40 m/s 超）を除外して別の DataFrame に保存
    filtered_df = move_df[move_df['P_speed'] <= 30]\
                    .groupby('move_id')\
                    .filter(lambda x: len(x) >= 3)

    filtered_df = filtered_df[["hashed_adid", "move_id", "datetime", "latitude_anonymous", "longitude_anonymous",  "accuracy"]]\
                    .assign(
                        lat_prev=lambda x: x.groupby('move_id')['latitude_anonymous'].shift(1),
                        lon_prev=lambda x: x.groupby('move_id')['longitude_anonymous'].shift(1),
                        time_prev=lambda x: x.groupby('move_id')['datetime'].shift(1),
                        distance_m=lambda x: x.apply(lambda row: haversine_distance(
                            row['lat_prev'], 
                            row['lon_prev'], 
                            row['latitude_anonymous'], 
                            row['longitude_anonymous']
                        ) if pd.notna(row['lat_prev']) else 0, axis=1),
                        time_diff_s=lambda x: (x['datetime'] - x['time_prev']).dt.total_seconds(),
                        P_speed=lambda x: x['distance_m'] / x['time_diff_s'],
                        S_speed_avg=lambda x: x.groupby('move_id')['P_speed'].transform('mean')
                    )\
                    .drop(columns=["lat_prev", "lon_prev", "time_prev", "time_diff_s"])\
                    .reset_index(drop=True)

    return filtered_df


#滞在判定
def stay_detection(df, roam_dist, stay_dur, eps_meters, min_samples):
    stay_df = extract_stays_fast(df, roam_dist, stay_dur)
    clustered_stays = run_dbscan_on_stays(stay_df, eps_meters, min_samples)
    stay_ad_df = assign_stay_ids(df, clustered_stays)
    move_df = stay_to_move(stay_ad_df)
    speed_GPS = speed_calc(move_df)
    move_summary = func_move_summary(speed_GPS)
    return clustered_stays, move_summary, speed_GPS