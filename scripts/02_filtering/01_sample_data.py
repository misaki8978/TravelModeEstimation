#! /usr/bin/env python3
import os
import sys
import pandas as pd
import gzip
from datetime import datetime, timedelta
import warnings
import multiprocessing
from functools import partial
sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

warnings.filterwarnings('ignore')


# --- 引数解析 ---
args = sys.argv[1:-2]


split_idx = args.index('--')
filter_files, gps_files = args[:split_idx], args[split_idx + 1:]
first_file = os.path.basename(gps_files[0])
file_number = os.path.splitext(os.path.splitext(first_file)[0])[0]
_path = os.path.dirname(gps_files[-1])
path_parts = _path.split("/")
# 最後の2つの要素を取得
input_folder = path_parts[-1]

place = sys.argv[-1]
year = sys.argv[-2]
# gps_files = args[split_idx + 1:]
message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_{place}.txt"

# log_message(f"place: {place}, year: {year}, input_folder: {input_folder}",message_path)


OUT_DIR = f"/home/data/fukui/interim/filtered/{place}/{year}_weekly/user_counts_4500/bulk/"
os.makedirs(OUT_DIR, exist_ok=True)

# message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_{place}.txt"

weekly_filter = pd.concat(
                          [pd.read_csv(f, compression="gzip") for f in filter_files], 
                          axis='index',
                          ignore_index=True
                         )\
                  .assign(
                    week_start=lambda x: pd.to_datetime(x['week_start']).dt.date
                  )

# weekly_filter['week_start'] = pd.to_datetime(weekly_filter['week_start']).dt.date

# log_message(f"{weekly_filter.head()}")

weekly_records = []
#filterにかけてそのぶんだけ抽出
def process_chunk(
    f: str,
    weekly_filter: pd.DataFrame
):
    df = pd.read_csv(f, compression="gzip")\
        .loc[lambda x: x['hashed_adid'].notna()]\
        .assign(
            datetime=lambda x: pd.to_datetime(x['datetime'], errors='coerce'),
            week_start=lambda x: x['datetime'].dt.date - pd.to_timedelta((x['datetime'].dt.weekday + 1) % 7, unit='d')
        )\
        .merge(weekly_filter[['hashed_adid', 'week_start']], on=['hashed_adid', 'week_start'], how='inner')\
        .drop(columns=["uuid", 'mesh', 'os'])
    return df

process_func = partial(
                        process_chunk,
                        weekly_filter=weekly_filter
                        )

tasks = gps_files

with multiprocessing.Pool(processes=8) as pool:
    results = pool.map(
        process_func, tasks
    )   
for res in results:
    weekly_records.append(res)

# 1. より効率的なDataFrame結合（concatの代わりにリスト内包表記を使用）
if weekly_records:
    # 空のDataFrameを除外してから結合
    valid_records = [df for df in weekly_records if not df.empty]
    if valid_records:
        final_result = pd.concat(valid_records, ignore_index=True)
        
        
        log_message(f"final_result: {len(final_result)}", message_path)
        
        
        chunk_size = 1600000
        #log_message(f"file_number: {file_number}")
        for i in range(0, len(final_result), chunk_size):
            chunk = final_result[i:i + chunk_size]
            output_file_name = f"{file_number}_raw_weekly_user_counts_{i // chunk_size + 1}.csv.gz"
            chunk.to_csv(f"{OUT_DIR}{output_file_name}", index=False, compression='gzip')
            log_message(f"Saved weekly_user_counts.csv with {len(chunk)} rows / {final_result.shape[0]} rows.", message_path)
    
