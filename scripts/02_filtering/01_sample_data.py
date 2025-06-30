#! /usr/bin/env python3
import os
import sys
import pandas as pd
import gzip
from datetime import datetime, timedelta
import warnings

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

warnings.filterwarnings('ignore')


# --- 引数解析 ---
args = sys.argv[1:-2]
if '--' not in args or len(args) < 3:
    log_message(
        f"Usage: python gps_data_filter.py <filter_files> -- <gps_files>", 
        f"/home/fukui/workspace/TravelModeEstimation/logs/log_sample_data.txt"
    )
    sys.exit(1)

split_idx = args.index('--')
filter_files, gps_files = args[:split_idx], args[split_idx + 1:]

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
                          [pd.read_csv(f) for f in filter_files], 
                          axis='index',
                          ignore_index=True
                         )\
                  .assign(
                    week_start=lambda x: pd.to_datetime(x['week_start']).dt.date
                  )

# weekly_filter['week_start'] = pd.to_datetime(weekly_filter['week_start']).dt.date

# log_message(f"{weekly_filter.head()}")

weekly_records = []
for filename in gps_files:
    try:
        with gzip.open(filename, 'rt') as f:
            df = pd.read_csv(f)\
                .loc[lambda x: x['hashed_adid'].notna()]\
                .assign(
                    datetime=lambda x: pd.to_datetime(x['datetime'], errors='coerce'),
                    week_start=lambda x: x['datetime'].dt.date - pd.to_timedelta((x['datetime'].dt.weekday + 1) % 7, unit='d')
                )\
                .merge(weekly_filter[['hashed_adid', 'week_start']], on=['hashed_adid', 'week_start'], how='inner')
            weekly_records.append(df)
            # log_message(f"{df.shape[0]}")
            f.close()
    except FileNotFoundError:
        log_message(
            f"ファイル {filename} が見つかりませんでした。", 
            message_path
        )

if weekly_records:
    result = pd.concat(
                       weekly_records, ignore_index=True
                       )\
               .reset_index(drop=True)

    # ファイル名を取得
    first_file = os.path.basename(gps_files[0])
    first_file_number = os.path.splitext(first_file)[0]
    file_number = os.path.splitext(first_file_number)[0]

    chunk_size = 1600000
    #log_message(f"file_number: {file_number}")
    for i in range(0, len(result), chunk_size):
        chunk = result[i:i + chunk_size]
        output_file_name = f"{input_folder}_{file_number}_4500_gps_{(i // chunk_size + 1):02d}.csv.gz"
        chunk.to_csv(f"{OUT_DIR}/{output_file_name}", index=False, compression='gzip')
        log_message(f"Saved weekly_gps.csv with {len(chunk)} rows / {result.shape[0]} rows.", message_path)
else:
    log_message("No valid data processed.", message_path)
    
