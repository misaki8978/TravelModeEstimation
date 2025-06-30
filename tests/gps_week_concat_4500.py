import pandas as pd
import os
import sys
import gzip

# ---------- 設定 ----------
THRESHOLD = 4500
OUT_DIR = "./../../out/GPS/weeklyfreq/"
folder_name = "09_nagasaki_2019"  #ここを変更！！！

os.makedirs(OUT_DIR, exist_ok=True)

def log_message(message):
    with open("log_concat.txt", "a") as log_file:
        log_file.write(message + "\n")

file_list = sys.argv[1:]  # CSV ファイル（すでに週単位で集計済み）を指定
dfs = []

for file in file_list:
    try:
        with gzip.open(file, 'rt') as f:
            df = pd.read_csv(file)
            dfs.append(df)
            # log_message("OK")
    except Exception as e:
        log_message(f"[ERROR] {file}: {e}")

if not dfs:
    log_message("読み込み可能なファイルがありません。")
    sys.exit(1)

# 1) 結合
all_df = pd.concat(dfs, ignore_index=True)

# 2) 合算（同一ユーザー×週）
grouped = (
    all_df.groupby(['hashed_adid', 'week_start'], as_index=False)
          .sum(numeric_only=True)
)

# 3) 4500 以上のユーザー×週のみ抽出
filtered = grouped[grouped['count'] >= THRESHOLD]
# log_message("filter OK")

# filtered.to_csv( "./../../out/GPS/weeklyfreq/data_10/weekly_freq.csv", index=False)
# 4) 出力
chunk_size = 1600000
#log_message(f"file_number: {file_number}")
for i in range(0, len(filtered), chunk_size):
    chunk = filtered[i:i + chunk_size]
    output_file_name = f"{folder_name}_weekly_user_counts_{i // chunk_size + 1}.csv.gz"
    # log_message("filename OK")
    chunk.to_csv( f"{OUT_DIR}" + output_file_name, index=False, compression='gzip')
    log_message(f"Saved weekly_user_counts.csv with {len(chunk)} rows.")
