#! /usr/bin/env python3
import os
import sys
import pandas as pd
import gzip
from datetime import datetime, timedelta
from collections import defaultdict
import warnings
import multiprocessing
from functools import partial
sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

warnings.filterwarnings('ignore')

file_list = sys.argv[1:]  # 引数でファイルリストを受け取る

message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_02_filtering.txt"

_path = os.path.dirname(file_list[0])
upper_path = os.path.dirname(_path)

OUT_DIR = f"{upper_path}/sorted/"
os.makedirs(OUT_DIR, exist_ok=True)

# 月ごとにDataFrameをまとめる辞書（キー: 'YYYY-MM'、値: DataFrameのリスト）
monthly_data = defaultdict(list)

for filename in file_list:
    try:
        with gzip.open(filename, 'rt') as f:
            df = pd.read_csv(f)\
                .loc[lambda x: x['hashed_adid'].notna()]\
                .assign(
                    datetime=lambda x: pd.to_datetime(x['datetime'], errors='coerce'))
            # 月ごとにグループ分けして、辞書に追加
            for month, group in df.groupby(df['datetime'].dt.to_period('M')):
                monthly_data[str(month)].append(group.reset_index(drop=True))
            f.close()
    except Exception as e:
        log_message(f"Error reading file {filename} : {e}", message_path)
def process_chunk(
    f: str
    ):
    monthly_data = defaultdict(list)
    df = pd.read_csv(f)\
                .loc[lambda x: x['hashed_adid'].notna()]\
                .assign(
                    datetime=lambda x: pd.to_datetime(x['datetime'], errors='coerce'))
    # 月ごとにグループ分けして、辞書に追加
    for month, group in df.groupby(df['datetime'].dt.to_period('M')):
        monthly_data[str(month)].append(group.reset_index(drop=True))
    return monthly_data
monthly_data = defaultdict(list)
with multiprocessing.Pool(processes=8) as pool:
    results = pool.map(
        process_chunk, file_list
    )   


# 各月のリストに統合
monthly_data = defaultdict(list)
for res in results:
    for month, frames in res.items():
        monthly_data[month].extend(frames)


for month, frames in monthly_data.items():
    if not frames:
        continue
    month_df = pd.concat(frames, ignore_index=True)
    chunk_size = 1600000
    #log_message(f"file_number: {file_number}")
    for i in range(0, len(month_df), chunk_size):
        chunk = month_df[i:i + chunk_size]
        output_file_name = f"{month}_sorted_{i // chunk_size + 1}.csv.gz"
        chunk.to_csv(f"{OUT_DIR}{output_file_name}", index=False, compression='gzip')
        log_message(f"Saved {month}_sorted.csv with {len(chunk)} rows / {month_df.shape[0]} rows.")



