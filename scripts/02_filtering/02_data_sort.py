#! /usr/bin/env python3
import os
import sys
import pandas as pd
import gzip
from datetime import datetime, timedelta
from collections import defaultdict
import warnings

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

warnings.filterwarnings('ignore')

file_list = sys.argv[1:]  # 引数でファイルリストを受け取る

message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_02_filtering.txt"

_path = os.path.dirname(file_list[0])
upper_path = os.path.dirname(_path)

OUT_DIR = f"{upper_path}/sorted"
os.makedirs(OUT_DIR, exist_ok=True)

# 月ごとにDataFrameをまとめる辞書（キー: 'YYYY-MM'、値: DataFrameのリスト）
monthly_data = defaultdict(list)

for filename in file_list:
    try:
        with gzip.open(filename, 'rt') as f:
            df = pd.read_csv(f)\
                .loc[lambda x: x['hashed_adid'].notna()]\
                .assign(
                    datetime=lambda x: pd.to_datetime(x['datetime'], errors='coerce'),
                    week_start=lambda x: x['datetime'].dt.date - pd.to_timedelta((x['datetime'].dt.weekday + 1) % 7, unit='d')
                )
            # 月ごとにグループ分けして、辞書に追加
            for month, group in df.groupby(df['datetime'].dt.to_period('M')):
                monthly_data[str(month)].append(group.reset_index(drop=True))
            f.close()
    except Exception as e:
        log_message(f"Error reading file {filename} : {e}", message_path)

# 各月のデータを1つのDataFrameに結合
for month in monthly_data:
    monthly_data[month] = pd.concat(
                                monthly_data[month], 
                                ignore_index=True
                                )\
                            .sort_values(by=['hashed_adid', 'datetime'])\
                            .reset_index(drop=True)
    monthly_data[month].to_csv(
                            f"{OUT_DIR}/{month}_sorted_gps_data.csv.gz", 
                            index=False, 
                            compression='gzip'
                            )
    log_message(f"Saved {month}_sorted_gps_data.csv.gz with {monthly_data[month].shape[0]} rows.", message_path)



