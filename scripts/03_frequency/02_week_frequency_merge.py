#! /usr/bin/env python3
import pandas as pd
import gzip
import os
import sys
from datetime import datetime, timedelta
import multiprocessing
import warnings
warnings.filterwarnings('ignore')

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/log_03_week_frequency_merge.txt"

# 閾値
THRESHOLD = 4500


file_list = sys.argv[1:]  # 引数でファイルリストを受け取る

_path = os.path.dirname(file_list[0])
folder_name = _path.split("agg_before_filter/")[1].split("/")[0]


# フォルダ名からplaceとyearを取得
place, year = folder_name.rsplit("_", 1)

OUT_DIR = f"/home/data/fukui/interim/agg_before_filter/{folder_name}/merged"
FILTER_DIR = f"/home/data/fukui/interim/filter/{place}/{year}_weekly/"
os.makedirs(OUT_DIR, exist_ok=True)



# log_message(f"{folder_name}")
def process_chunk(file_list):
    dfs = []
    for file in file_list:
        df = pd.read_csv(file, compression="gzip")
        dfs.append(df)
 
    result = pd.concat(
                dfs, ignore_index=True
                    )\
            .groupby(# 同一ユーザー×週のデータが複数にまたがる場合を考慮して合算
                    by=['hashed_adid', 'week_start'], as_index=False
                    ).sum()
    log_message(f"result: {len(result)}", log_path)
    return result

weekly_records = []
list_len = 100
tasks = [file_list[i:i + list_len] for i in range(0, len(file_list), list_len)]
log_message(f"tasks: {len(tasks)}", log_path)

with multiprocessing.Pool(processes=8) as pool:
    results = pool.map(
        process_chunk, tasks
    )   
for res in results:
    weekly_records.append(res)


# 1. より効率的なDataFrame結合（concatの代わりにリスト内包表記を使用）
if weekly_records:
    # 空のDataFrameを除外してから結合
    valid_records = [df for df in weekly_records if not df.empty]
    if valid_records:
        df_weekly_record = pd.concat(valid_records, ignore_index=True)
        
        # 2. グループ化と集計を最適化
        final_result = (df_weekly_record
                       .groupby(['hashed_adid', 'week_start'], as_index=False)
                       .agg({'count': 'sum'}))
        
        log_message(f"final_result: {len(final_result)}", log_path)
        
        
        chunk_size = 1600000
        #log_message(f"file_number: {file_number}")
        for i in range(0, len(final_result), chunk_size):
            chunk = final_result[i:i + chunk_size]
            output_file_name = f"raw_weekly_user_counts_{i // chunk_size + 1}.csv.gz"
            chunk.to_csv(f"{OUT_DIR}/{output_file_name}", index=False, compression='gzip')
            log_message(f"Saved weekly_user_counts.csv with {len(chunk)} rows / {final_result.shape[0]} rows.", log_path)

        # Filter users with weekly counts in the inclusive range [THRESHOLD, 7000]
        weekly_4500 = final_result[(final_result['count'] >= THRESHOLD) & (final_result['count'] <= 7000)]

os.makedirs(FILTER_DIR, exist_ok=True)
for i in range(0, len(weekly_4500), chunk_size):
    chunk = weekly_4500[i:i + chunk_size]
    output_file_name = f"filter_4500_7000_{(i // chunk_size + 1):02d}.csv.gz"
    # log_message(f"Saved weekly_user_counts.csv with {len(chunk)} rows.")
    chunk.to_csv(FILTER_DIR +"/" + output_file_name, index=False, compression='gzip')
    log_message(f"Saved 4500_weekly_user_counts.csv with {len(chunk)} rows / {weekly_4500.shape[0]} rows.")