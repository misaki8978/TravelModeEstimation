#! /usr/bin/env python3
import pandas as pd
import gzip
import os
import sys
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')


file_list = sys.argv[1:]  # 引数でファイルリストを受け取る

_path = os.path.dirname(file_list[0])
folder_name = _path.split("agg_before_filter/")[1].split("/")[0]
OUT_DIR = f"/home/data/fukui/interim/agg_before_filter/{folder_name}/merged"

def log_message(message):
    with open(f"/home/fukui/workspace/TravelModeEstimation/logs/log_{folder_name}.txt", "a") as log_file:
        log_file.write(message + "\n")

dfs = []

log_message(f"{folder_name}")

for file in file_list:
    try:
        with gzip.open(file, 'rt') as f:
            df = pd.read_csv(f)
            dfs.append(df)
            f.close()
            # log_message("OK")
    except Exception as e:
        log_message(f"[ERROR] {f}: {e}")

if not dfs:
    log_message("読み込み可能なファイルがありません。")
    sys.exit(1)

result = pd.concat(
                dfs, ignore_index=True
                    )\
            .groupby(# 同一ユーザー×週のデータが複数にまたがる場合を考慮して合算
                    by=['hashed_adid', 'week_start'], as_index=False
                    ).sum()
os.makedirs(OUT_DIR, exist_ok=True)
chunk_size = 1600000
#log_message(f"file_number: {file_number}")
for i in range(0, len(result), chunk_size):
    chunk = result[i:i + chunk_size]
    output_file_name = f"weekly_user_counts_{i // chunk_size + 1}.csv.gz"
    # log_message(f"Saved weekly_user_counts.csv with {len(chunk)} rows.")
    chunk.to_csv(OUT_DIR +"/" + output_file_name, index=False, compression='gzip')
    log_message(f"Saved weekly_user_counts.csv with {len(chunk)} rows / {result.shape[0]} rows.")