#! /usr/bin/env python3
import pandas as pd
import gzip
import os
import sys
from datetime import datetime, timedelta
import warnings
import gzip
import multiprocessing
from functools import partial
warnings.filterwarnings('ignore')

def log_message(message):
    with open(f"/home/fukui/workspace/TravelModeEstimation/logs/{folder_name}/log_{folder_name}.txt", "a") as log_file:
        log_file.write(message + "\n")

file_list = sys.argv[1:]
folder_name = os.path.basename(os.path.dirname(file_list[0]))
chunk_size = 100000  # メモリ使用量を制御するためのチャンクサイズ

# 最終的な集計
first_file = os.path.basename(file_list[0])
file_number = os.path.splitext(os.path.splitext(first_file)[0])[0]
output_chunk_size = 1600000

# 結果を保存するディレクトリを作成
output_dir = f"/home/data/fukui/interim/agg_before_filter/{folder_name}/bulk"
os.makedirs(output_dir, exist_ok=True)

# # 一時ファイルを保存するディレクトリ
# temp_dir = f"/home/data/fukui/interim/temp/{folder_name}"
# os.makedirs(temp_dir, exist_ok=True)

def process_chunk(file):
    df = pd.read_csv(file, compression="gzip")\
            .assign(
                       datetime=lambda x: pd.to_datetime(x['datetime'], errors='coerce'),
                       # 週の開始日（日曜）を計算
                       week_start=lambda x: x['datetime'].dt.date - pd.to_timedelta((x['datetime'].dt.weekday + 1) % 7, unit='d')
                   )\
            .groupby(['hashed_adid', 'week_start'])\
            .size()\
            .reset_index(name='count')
    # log_message(f"{file_number} df: {df.head()}")
    return (df)




weekly_records = []
tasks = file_list

with multiprocessing.Pool(processes=8) as pool:
    results = pool.map(
        process_chunk, tasks
    )   
for res in results:
    weekly_records.append(res)
        

# 高速化された出力処理
log_message(f"{file_number} len(weekly_records): {len(weekly_records)}")

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
        
        log_message(f"{file_number} final_result: {len(final_result)}")
        
        
        chunk_size = 1600000
        #log_message(f"file_number: {file_number}")
        for i in range(0, len(final_result), chunk_size):
            chunk = final_result[i:i + chunk_size]
            output_file_name = f"{file_number}_weekly_user_counts_{i // chunk_size + 1}.csv.gz"
            chunk.to_csv(f"{output_dir}/{output_file_name}", index=False, compression='gzip')
            log_message(f"Saved weekly_user_counts.csv with {len(chunk)} rows / {final_result.shape[0]} rows.")

        
        log_message(f"{file_number} Saved.")
    else:
        log_message(f"{file_number} No valid records to save.")
else:
    log_message(f"{file_number} No records to process.")

