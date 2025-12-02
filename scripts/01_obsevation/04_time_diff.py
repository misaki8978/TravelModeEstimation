import os
import pandas as pd
import matplotlib.pyplot as plt
import gzip
import sys
import warnings
plt.rcParams['font.size'] = 15
# import japanize_matplotlib


sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

warnings.filterwarnings('ignore')

message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_time_diff.txt"

files = sys.argv[1:]  # 引数でファイルリストを受け取る
# log_message(f"files: {files[0]}", message_path)

_path = os.path.dirname(files[0])
path_parts = _path.split("/")
# 最後の2つの要素を取得
year = path_parts[-3].split("_")[-2]
place = path_parts[-4]
OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/{place}/{year}/04_time_diff"
os.makedirs(OUT_DIR, exist_ok=True)

weekly_records = []
for file in files:
    try:
        with gzip.open(file, 'rt') as f:
            df = pd.read_csv(f, parse_dates=["datetime"])
            # log_message(f"{(df.shape[0])}", message_path)
            weekly_records.append(df[['hashed_adid', 'datetime']])
            f.close()
    except FileNotFoundError:
        log_message(f"ファイル {file} が見つかりませんでした。", message_path)

df = pd.concat(weekly_records, ignore_index=True)
# log_message(f"df.columns: {df.columns}", message_path)
# 全ユーザーの時間間隔を計算
df = df.groupby("hashed_adid", group_keys=False)\
        .apply(lambda x: x.sort_values("datetime"))\
        .assign(
            time_diff_sec=lambda x: x['datetime'].diff().dt.total_seconds()
        )
all_time_diffs = df['time_diff_sec'].dropna()\
                                    .value_counts()\
                                    .sort_index()\
                                    .reset_index()\
                                    .rename(columns={'count': 'frequency'})


log_message(f"all_time_diffs: {all_time_diffs.head()}", message_path)

# プロット
fig, ax = plt.subplots(figsize=(10, 6))
ax.scatter(all_time_diffs['time_diff_sec'], all_time_diffs['frequency'], 
           alpha=0.6, color="skyblue", s=10)
ax.set_title("Time Interval Frequency Distribution")
ax.set_xlabel("Time Interval (seconds)")
ax.set_ylabel("Frequency")
# ax.set_xlim(0, 600)  # 必要なら有効化
ax.set_xscale("log")
ax.set_yscale("log")
ax.grid(True)

plt.savefig(f"{OUT_DIR}/04_time_diff_distribution.png")
# plt.close()