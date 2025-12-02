import os
import sys
import pandas as pd
import geopandas as gpd

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from gis import division_several_file, train_from_OSM, make_buffer, is_in_buffer, is_in_buffer_series,_covers_flag, file_to_gdf, make_features, make_features_walk




# file = sys.argv[1]
files = sys.argv[1:]
# log_message(f"{files}", log_path)

#バスのデータフレームとファイルを取得
bus_gdf, rail_gdf, file, year, place = division_several_file(files) #geopandasのデータフレームに変換
# os.makedirs(f"/home/fukui/workspace/TravelModeEstimation/logs/09_gis", exist_ok=True)
log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/09_gis/{place}_{year}.txt"
OUT_PROCESSED = f"/home/data/fukui/processed/09_04_re/{place}/{year}"
os.makedirs(OUT_PROCESSED+"/GPS", exist_ok=True)

# #鉄道ネットワークを取得
# target_place = "Osaka, Japan"
# train_gdf = train_from_OSM(target_place)

#バッファーを作成
train_buffer = make_buffer(rail_gdf, 30)
bus_buffer = make_buffer(bus_gdf, 30)

#ファイルごとにバッファー内にあるかどうかを確認
month_list = []
walk_list = []
for f in file:
    segment_gdf, walk_df, month = file_to_gdf(f)
    if str(segment_gdf.crs).lower() != "epsg:32652":
        segment_gdf = segment_gdf.to_crs(32652)
    gdf_isin = (
        segment_gdf
        .assign(
            train_in_buffer = _covers_flag(segment_gdf, train_buffer),
            bus_in_buffer   = _covers_flag(segment_gdf, bus_buffer),
        )
        .assign(
            train_rate = lambda d: d.groupby(['hashed_adid','segment_month_id'])['train_in_buffer'].transform('mean'),
            bus_rate   = lambda d: d.groupby(['hashed_adid','segment_month_id'])['bus_in_buffer'].transform('mean'),
        )
    )
    gdf_isin.to_csv(f"{OUT_PROCESSED}/GPS/{month}_gdf_isin.csv.gz", index=False, compression="gzip")
    month_list.append(gdf_isin)
    walk_list.append(walk_df)


gdf_isin = pd.concat(month_list)
gdf_isin.to_csv(f"{OUT_PROCESSED}/gdf_isin.csv.gz", index=False, compression="gzip")
log_message(f"train: {gdf_isin['train_in_buffer'].value_counts()} GPS points", log_path)
log_message(f"bus: {gdf_isin['bus_in_buffer'].value_counts()} GPS points", log_path)
walk_gdf = pd.concat(walk_list)
log_message(f"{len(gdf_isin)} non-walk GPS points", log_path)
log_message(f"{len(walk_gdf)} walk GPS", log_path)
walk_gdf.to_csv(f"{OUT_PROCESSED}/walk_gdf.csv.gz", index=False, compression="gzip")


# file_nonwalk = files[0]
# file_walk = files[1]
# df_isin = pd.read_csv(file_nonwalk, compression="gzip", parse_dates=['datetime'])
# gdf_isin = gpd.GeoDataFrame(df_isin, geometry=gpd.points_from_xy(df_isin.longitude_anonymous, df_isin.latitude_anonymous), crs="EPSG:4326")
# place = "_".join(file_nonwalk.split("/")[-3].split("_")[-2:])
# year = file_nonwalk.split("/")[-2]
# OUT_PROCESSED = f"/home/data/fukui/processed/09_04_re/{place}/{year}"
# os.makedirs(OUT_PROCESSED, exist_ok=True)


# gdf_walk = pd.read_csv(file_walk, compression="gzip", parse_dates=['datetime'])
# walk_gdf = gpd.GeoDataFrame(gdf_walk, geometry=gpd.points_from_xy(gdf_walk.longitude_anonymous, gdf_walk.latitude_anonymous), crs="EPSG:4326")
# log_message(f"{walk_gdf.head()}", log_path)

# log_message(f"{df_isin.columns}", log_path)
#特徴量データフレームを作成
gdf_features = make_features(gdf_isin)
gdf_features.to_csv(f"{OUT_PROCESSED}/segment_gis_features.csv.gz", index=False, compression="gzip")
gdf_features_walk = make_features_walk(walk_gdf)
log_message(f"{gdf_features.columns}", log_path)
# log_message(f"{gdf_features["bearing_change_rate"].describe()}", log_path)
# log_message(f"{gdf_features["stop_rate"].describe()}", log_path)
gdf_walk_nonwalk = pd.concat([gdf_features, gdf_features_walk])
gdf_walk_nonwalk.to_csv(f"{OUT_PROCESSED}/segment_gis_features_walk_nonwalk.csv.gz", index=False, compression="gzip")
# log_message(f"all: {len(gdf_features)} segments", log_path)
# log_message(f"rail: {len(gdf_features.query('rail_flag == 1'))} segments", log_path)
# log_message(f"bus: {len(gdf_features.query('bus_flag == 1'))} segments", log_path)


log_message(f"{gdf_walk_nonwalk['is_walk'].value_counts()}", log_path)

