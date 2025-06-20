#! /usr/bin/env python3
import pandas as pd
import gzip
import os
import sys
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

file_list = sys.argv[1:]  # 引数でファイルリストを受け取る
#foldername
folder_name = os.path.basename(os.path.dirname(file_list[0]))

weekly_records = []  # 週ごとの出現数記録を格納

def log_message(message):
    with open(f"log_test.txt", "a") as log_file:
        log_file.write(message + "\n")


for filename in file_list:
    try:
        # log_message(f"{filename}を処理中...")
        with gzip.open(filename, 'rt') as f:
            df = pd.read_csv(f)\
                   .loc[lambda x: x['hashed_adid'].notna()]\
                   .assign(
                       datetime=lambda x: pd.to_datetime(x['datetime'], errors='coerce'),
                       # 週の開始日（日曜）を計算
                       week_start=lambda x: x['datetime'].dt.date - pd.to_timedelta((x['datetime'].dt.weekday + 1) % 7, unit='d')
                   )\
                   .groupby(
                        by=['hashed_adid', 'week_start']
                        ).size()\
                   .reset_index(name='count')
            weekly_records.append(df)
            f.close()
    except FileNotFoundError:
        log_message(f"ファイル {filename} が見つかりませんでした。")

# 全ファイルの集計結果を結合
if weekly_records:
    result = pd.concat(
                       weekly_records, ignore_index=True
                       )\
               .groupby(# 同一ユーザー×週のデータが複数にまたがる場合を考慮して合算
                        by=['hashed_adid', 'week_start'], as_index=False
                        ).sum()
    
    #ファイルナンバー取得
    first_file = os.path.basename(file_list[0])  # パスを除いたファイル名を取得
    first_file_number = os.path.splitext(first_file)[0]  # 拡張子を除いたファイル名部分
    file_number = os.path.splitext(first_file_number)[0]  # 拡張子を除いたファイル名部分
    # 保存
    
    os.makedirs(f"/home/data/fukui/interim/agg_before_filter/{folder_name}/bulk", exist_ok=True)
    chunk_size = 1600000
    #log_message(f"file_number: {file_number}")
    for i in range(0, len(result), chunk_size):
        chunk = result[i:i + chunk_size]
        output_file_name = f"{file_number}_weekly_user_counts_{i // chunk_size + 1}.csv.gz"
        chunk.to_csv(f"/home/data/fukui/interim/agg_before_filter/{folder_name}/bulk/" + output_file_name, index=False, compression='gzip')
        log_message(f"Saved weekly_user_counts.csv with {len(chunk)} rows / {result.shape[0]} rows.")
else:
    log_message("No valid data processed.")
