#! /usr/bin/env python3
import pandas as pd
import gzip
import os
import sys
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def log_message(message):
    with open(f"/home/fukui/workspace/TravelModeEstimation/logs/{folder_name}/log_{folder_name}_{file_number}.txt", "a") as log_file:
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

# 一時ファイルを保存するディレクトリ
temp_dir = f"/home/data/fukui/interim/temp/{folder_name}"
os.makedirs(temp_dir, exist_ok=True)

def process_chunk(chunk):
    return (chunk
            .loc[lambda x: x['hashed_adid'].notna()]
            .assign(
                datetime=lambda x: pd.to_datetime(x['datetime'], errors='coerce'),
                week_start=lambda x: x['datetime'].dt.date - pd.to_timedelta((x['datetime'].dt.weekday + 1) % 7, unit='d')
            )
            .groupby(['hashed_adid', 'week_start'])
            .size()
            .reset_index(name='count'))

temp_files = []
for i, filename in enumerate(file_list):
    try:
        log_message(f"Processing file {i+1}/{len(file_list)}: {filename}")
        temp_file = f"{temp_dir}/temp_{i}.csv.gz"
        temp_files.append(temp_file)
        
        # チャンク単位で処理
        chunks_processed = 0
        chunk_results = []
        
        with gzip.open(filename, 'rt') as f:
            for chunk in pd.read_csv(f, chunksize=chunk_size):
                processed_chunk = process_chunk(chunk)
                chunk_results.append(processed_chunk)
                chunks_processed += 1
                
                # メモリを節約するため、一定数のチャンクが処理されたら中間結果を保存
                if len(chunk_results) >= 10:
                    intermediate = pd.concat(chunk_results).groupby(['hashed_adid', 'week_start'], as_index=False).sum()
                    intermediate.to_csv(temp_file, index=False, compression='gzip', mode='a' if chunks_processed > 10 else 'w')
                    chunk_results = []
                    
        # 残りのチャンクを処理
        if chunk_results:
            intermediate = pd.concat(chunk_results).groupby(['hashed_adid', 'week_start'], as_index=False).sum()
            intermediate.to_csv(temp_file, index=False, compression='gzip', mode='a' if chunks_processed > 0 else 'w')
            
    except FileNotFoundError:
        log_message(f"File {filename} not found.")
        continue

# 一時ファイルを順次読み込んで集計
final_result = pd.DataFrame()
for temp_file in temp_files:
    if os.path.exists(temp_file):
        chunk_df = pd.read_csv(temp_file, compression='gzip')
        if final_result.empty:
            final_result = chunk_df
        else:
            final_result = pd.concat([final_result, chunk_df]).groupby(['hashed_adid', 'week_start'], as_index=False).sum()
        # 中間ファイルを削除
        os.remove(temp_file)

# 結果を指定されたチャンクサイズで保存
for i in range(0, len(final_result), output_chunk_size):
    chunk = final_result[i:i + output_chunk_size]
    output_file_name = f"{file_number}_weekly_user_counts_{i // output_chunk_size + 1}.csv.gz"
    output_path = os.path.join(output_dir, output_file_name)
    chunk.to_csv(output_path, index=False, compression='gzip')
    log_message(f"Saved {output_file_name} with {len(chunk)} rows / {final_result.shape[0]} rows.")

# 一時ディレクトリの削除
os.rmdir(temp_dir)
log_message("Processing completed successfully.")
