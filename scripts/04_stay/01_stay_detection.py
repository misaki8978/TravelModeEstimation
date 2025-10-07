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
import multiprocessing as mp
sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from stay_func import stay_detection, extract_stays_fast, run_dbscan_on_stays, assign_stay_ids, stay_to_move, speed_calc, func_move_summary
from stay_func_re import stayPointExtraction, Point

warnings.filterwarnings("ignore")

# ===== グローバル変数 =====
base_dict = {}
results_prev_dict = {}
func_name_global = None
month_global = None
message_path = None


def init_worker(results_prev, func_name, month, msg_path):
    global results_prev_dict, func_name_global, month_global, message_path
    results_prev_dict = results_prev
    func_name_global = func_name
    month_global = month
    message_path = msg_path


def worker(task_id):
    target_id = task_id
    roam_dist = 200
    stay_dur = 600
    eps_meters = 200
    min_samples = 2

    # if func_name_global == "extract_stays_fast":
    #     df = base_dict[target_id]
    #     log_message(f"extract_stays_fast: {month_global} {target_id}", message_path)
    #     return extract_stays_fast(df, roam_dist, stay_dur)
    if func_name_global == "stayPointExtraction":
        if target_id not in base_dict:
            return pd.DataFrame(columns=["hashed_adid","latitude","longitude","datetime"])
        df = base_dict[target_id]
        points = [
                    Point(row['latitude_anonymous'], row['longitude_anonymous'], row['datetime'], row['hashed_adid'])
                    for _, row in df.iterrows()
                ]
        log_message(f"stayPointExtraction: {month_global} {target_id}", message_path)
        return stayPointExtraction(points, roam_dist, stay_dur)

    elif func_name_global == "run_dbscan_on_stays":
        if target_id in results_prev_dict:
            # move_df = results_prev_dict[target_id]
            # log_message(f"run_dbscan_on_stays: {month_global} {target_id} length {len(move_df)}", message_path)
            # stay_cluster = run_dbscan_on_stays(move_df, eps_meters, min_samples)
            # log_message(f"stay_cluster: {target_id} {len(stay_cluster)}", message_path)
            
            return run_dbscan_on_stays(results_prev_dict[target_id], eps_meters, min_samples)
            
        else:
            # log_message(f"run_dbscan_on_stays: {month_global} {target_id} None", message_path)
            return pd.DataFrame(
                columns=["hashed_adid", "latitude", "longitude", "datetime", "cluster"]
            )

    elif func_name_global == "assign_stay_ids":
        if target_id in results_prev_dict:
            clustered_stays = results_prev_dict[target_id]
            if target_id not in base_dict:
                return pd.DataFrame(columns=["hashed_adid","latitude","longitude","datetime","stay_id"])
            df = base_dict[target_id]
            return assign_stay_ids(df, clustered_stays)
        else:
            if target_id not in base_dict:
                return pd.DataFrame(columns=["hashed_adid","latitude","longitude","datetime","stay_id"])
            df = base_dict[target_id]
            return df.assign(stay_id=-1)

    elif func_name_global == "stay_to_move":
        if target_id in results_prev_dict:
            stay_ad_df = results_prev_dict[target_id]
            return stay_to_move(stay_ad_df)
        else:
            return None

    elif func_name_global == "speed_calc":
        if target_id in results_prev_dict:
            # move_df = results_prev_dict[target_id]
            return speed_calc(results_prev_dict[target_id])
        else:
            return None

    elif func_name_global == "func_move_summary":
        if target_id in results_prev_dict:
            # speed_GPS = results_prev_dict[target_id]
            return func_move_summary(results_prev_dict[target_id])
        else:
            return None

    else:
        raise ValueError(f"Unknown func_name: {func_name_global}")


if __name__ == "__main__":
    files = sys.argv[1:]
    message_path = "/home/fukui/workspace/TravelModeEstimation/logs/log_04_stay_multithread.txt"

    # path情報
    path_parts = files[0].split("/")
    place = path_parts[-5]
    year = path_parts[-4]
    month = path_parts[-1].split("_")[0]

    OUT_DIR = f"/home/data/fukui/interim/multithread/04_01_{place}_{year}/hariharan"
    # OUT_DIR = f"/home/data/fukui/interim/multithread/04_01_{place}_{year}/basic"

    os.makedirs(OUT_DIR, exist_ok=True)

    # ===== データ読み込み & dict化 =====
    df = pd.read_csv(files[0], parse_dates=["datetime"], compression="gzip") \
            .assign(
                latitude=lambda x: np.radians(x["latitude_anonymous"].astype(float)),
                longitude=lambda x: np.radians(x["longitude_anonymous"].astype(float))
            ).sort_values(by=["hashed_adid","datetime"], ascending=[True, True])
    base_dict = {k: v for k, v in df.groupby("hashed_adid")}
    # 大量のDataFrameを子プロセスへ送らず、IDのみ渡す
    tasks = list(base_dict.keys())

    funcs = [
        # "extract_stays_fast",
        "stayPointExtraction",
        "run_dbscan_on_stays",
        "assign_stay_ids",
        "stay_to_move",
        "speed_calc",
        "func_move_summary"
    ]

    results_prev_dict = {}

    for func_name in funcs:
        out_path = os.path.join(OUT_DIR, f"{func_name}_{month}.csv")

        if os.path.exists(out_path):
            log_message(f"[SKIP] {month} {func_name}", message_path)
            results_df = pd.read_csv(out_path)
            results_prev_dict = {k: v for k, v in results_df.groupby("hashed_adid")}
            continue

        # 並列処理
        with mp.Pool(processes=4, initializer=init_worker,
                     initargs=(results_prev_dict, func_name, month, message_path)) as pool:
            results = pool.map(worker, tasks)
            results = [r for r in results if r is not None]
        log_message(f"results: {len(results)}", message_path)
        results_df = pd.concat(results, ignore_index=True)
        results_df.to_csv(out_path, index=False)
        # log_message(f"results_df: {results_df.head()}", message_path)

        # 次のフェーズ用にdict化して保持
        results_prev_dict = {k: v for k, v in results_df.groupby("hashed_adid")}

        log_message(f"\n=== {month} {func_name} finished for ALL tasks ===\n", message_path)
