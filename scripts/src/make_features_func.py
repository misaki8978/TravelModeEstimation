import pandas as pd
import numpy as np
from geopy.distance import geodesic


def vincenty_distance(lat1, lon1, lat2, lon2):
    try:
        return geodesic((lat1, lon1), (lat2, lon2)).meters
    except:
        return np.nan

def calculate_bearing(lat1, lon1, lat2, lon2):
    # ラジアンに変換
    # lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    d_lon = lon2 - lon1
    y = np.sin(d_lon) * np.cos(lat2)
    x = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(d_lon)
    return np.arctan2(y, x) 

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000.0
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c

def make_seg_features(df_segment):

    valid_df = df_segment\
                        .sort_values(by=['hashed_adid', 'segment_id', 'datetime'])\
                        .groupby(['hashed_adid', 'segment_id'], group_keys=False)\
                        .filter(lambda g: len(g) >= 4)
    
    seg_features = valid_df\
        .sort_values(by=['hashed_adid', 'segment_id', 'datetime'])\
        .assign(
            lat_prev = lambda x: x['latitude_anonymous'].shift(1),
            lon_prev = lambda x: x['longitude_anonymous'].shift(1),
        )\
        .assign(
            distance = lambda x: x.apply(
                                        lambda row: haversine_distance(
                                                                        row['lat_prev'], 
                                                                        row['lon_prev'],
                                                                        row['latitude_anonymous'], 
                                                                        row['longitude_anonymous'],                                                                     
                                                                    ) if pd.notnull(row['lat_prev']) and pd.notnull(row['lon_prev']) else np.nan,
                                        axis=1
                                        ),
            bearing = lambda x: x.apply(
                                        lambda row: calculate_bearing(
                                            row['lat_prev'], row['lon_prev'], row['latitude_anonymous'], row['longitude_anonymous']
                                        ) if pd.notnull(row['lat_prev']) and pd.notnull(row['lon_prev']) else np.nan,
                                        axis=1
                                       ),
        )\
        .groupby(['hashed_adid', 'segment_id'], as_index=False)\
        .agg(
            label           = ('label', 'first'),
            n_points        = ('datetime', 'count'),
            mean_speed_mps  = ('speed', 'mean'),
            max_speed_mps   = ('speed', 'max'),
            min_speed_mps   = ('speed', 'min'),
            mean_accel_mps2 = ('acceleration', 'mean'),
            max_accel_mps2  = ('acceleration', 'max'),
            total_distance_m = ('distance', 'sum'),
            duration_sec    = ('datetime', lambda x: (x.max() - x.min()).total_seconds()),
            bearing_rate_rad = ('bearing', lambda b: np.mean(np.abs(np.diff(b.dropna().values))) if len(b.dropna()) >= 2 else np.nan)
        )
    return seg_features

