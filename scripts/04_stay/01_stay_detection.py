#! /usr/bin/env python3
import os
import sys
import pandas as pd
import gzip
from datetime import datetime, timedelta
import numpy as np
from functools import partial
from sklearn.cluster import DBSCAN
from scipy.spatial.distance import pdist
from math import radians, sin, cos, sqrt, atan2
import warnings
import multiprocessing

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from stay_func import stay_detection

warnings.filterwarnings("ignore")


files = sys.argv[1:]  # 引数でファイルリストを受け取る
message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_04_stay.txt"

path_parts = files[0].split("/")
# log_message(f"{path_parts}", message_path)
# 最後の2つの要素を取得
place = path_parts[-5]
year = path_parts[-4]
month = path_parts[-1].split("_")[0]  #'2019-11'

OUT_DIR = f"/home/data/fukui/processed/04_01_{place}/{year}/"
os.makedirs(OUT_DIR, exist_ok=True)

df = pd.read_csv(files[0], parse_dates=["datetime"])\
        .assign(
                latitude_anonymous=lambda x: np.radians(x["latitude_anonymous"]),
                longitude_anonymous=lambda x: np.radians(x["longitude_anonymous"]),
                )

    # log_message(f"{df.shape[0]}", message_path)

roam = 500
dur = 600
meters = 500
samples = 2

stays_list = []
moves_list = []
GPS_list = []

# Parallel processing for stay detection
tasks = df[["hashed_adid", "week_start"]].drop_duplicates().itertuples(index=False, name=None)
def _process_task(
    task: tuple,
    df: pd.DataFrame,
    roam_dist: int,
    stay_dur: int,
    eps_meters: int,
    min_samples: int,
):
    adid, week_start = task
    df = df.query("hashed_adid == @adid and week_start == @week_start")\
            .sort_values(
                        by=["datetime"], ignore_index=True
                        )
    
    if 0 < df.shape[0] < 7000:
        stays, moves, GPSs = stay_detection(
            df, roam_dist, stay_dur, eps_meters, min_samples
        )
        log_message(f"{month} {adid} {week_start} done", message_path)
        return stays, moves, GPSs
    return None


process_func = partial(
    _process_task,
    df=df,
    roam_dist=roam,
    stay_dur=dur,
    eps_meters=meters,
    min_samples=samples,
)


with multiprocessing.Pool(processes=32) as pool:
    results = pool.map(
        process_func, tasks
    )

for res in results:
    if res:
        stays, moves, GPSs = res
        stays_list.append(stays)
        moves_list.append(moves)
        GPS_list.append(GPSs)

log_message(f"{month} done", message_path)

stay_point_summary = pd.concat(stays_list, ignore_index=True)
move_summary = pd.concat(moves_list, ignore_index=True)
speed_GPS = pd.concat(GPS_list, ignore_index=True)

# log_message(f"{(speed_GPS.head())}", message_path)

stay_point_summary.to_csv(
    f"{OUT_DIR}/{month}_stay_point_summary.csv.gz", index=False, compression="gzip"
)
move_summary.to_csv(
    f"{OUT_DIR}/{month}_move_summary.csv.gz", index=False, compression="gzip"
)
speed_GPS.to_csv(
    f"{OUT_DIR}/{month}_speed_GPS.csv.gz", index=False, compression="gzip"
)
