import pandas as pd
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import os
import warnings
import gzip
sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

warnings.filterwarnings('ignore')

files = sys.argv[1:]  # 引数でファイルリストを受け取る
message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_02_obsevation.txt"


_path = os.path.dirname(files[0])
path_parts = _path.split("/")
# log_message(f"{path_parts}", message_path)
# 最後の2つの要素を取得
year = path_parts[-1]
place_ = path_parts[-2]
place = "_".join(place_.split("_")[2:])

OUT_DIR = "/home/data/fukui/outputs/figures/01_observation"
os.makedirs(OUT_DIR, exist_ok=True)


weekly_records = []

for file in files:
    try:
        with gzip.open(file, 'rt') as f:
            df = pd.read_csv(f)                
            # log_message(f"{(df.shape[0])}", message_path)
            weekly_records.append(df)
            f.close()
    except FileNotFoundError:
        log_message(f"ファイル {file} が見つかりませんでした。", message_path)

combined_stays = pd.concat(weekly_records, ignore_index=True)



# サブプロットの作成
fig, axs = plt.subplots(2, 3, figsize=(18, 10))  # 2行×3列
axs = axs.flatten()  # 1次元に変換

# 1. 1移動あたりのGPS点数の分布（scatter, 大きい順）
point_count_sorted = combined_stays['point_count'].sort_values(ascending=False).reset_index(drop=True)
axs[0].scatter(point_count_sorted.index, point_count_sorted.values, s=10)
axs[0].set_title('Point Count per Move', fontsize=17)
axs[0].set_xlabel('Rank')
axs[0].set_ylabel('Point Count', fontsize=13)

# 2. 移動距離の分布（scatter, 大きい順）
total_distance_sorted = combined_stays['total_distance_m'].sort_values(ascending=False).reset_index(drop=True)
axs[1].scatter(combined_stays['point_count'], total_distance_sorted.values, s=10)
axs[1].set_title('Total Distance per Move (m)', fontsize=17)
axs[1].set_xlabel('points per move', fontsize=13)
axs[1].set_ylabel('Distance (m)', fontsize=13)

# 3. 移動時間の分布（scatter, 大きい順）
move_duration_min_sorted = combined_stays['move_duration_sec'].sort_values(ascending=False).reset_index(drop=True)
axs[2].scatter(move_duration_min_sorted.index, move_duration_min_sorted.values, s=10)
axs[2].set_title('Move Duration (sec)', fontsize=17)
axs[2].set_xlabel('Rank')
axs[2].set_ylabel('Duration (minutes)', fontsize=13)

# 4. 平均速度の分布（scatter, 大きい順）
mean_speed_sorted = combined_stays['P_speed_avg'].sort_values(ascending=False).reset_index(drop=True)
axs[3].scatter(mean_speed_sorted.index, mean_speed_sorted.values, s=10)
axs[3].set_title('Mean Speed (m/s)', fontsize=17)
axs[3].set_xlabel('Rank')
axs[3].set_ylabel('Speed (m/s)', fontsize=13)

# 7. ユーザごとの移動回数分布（scatter, 大きい順）
user_move_counts_sorted = combined_stays['hashed_adid'].value_counts().sort_values(ascending=False).reset_index(drop=True)
axs[4].scatter(user_move_counts_sorted.index, user_move_counts_sorted.values, s=10)
axs[4].set_title('Number of Moves per User', fontsize=17)
axs[4].set_xlabel('Rank')
axs[4].set_ylabel('Number of Moves', fontsize=13)

# 6. Point Count vs Distance（散布図）
fig.delaxes(axs[5])
#.png出力
plt.savefig(f'{OUT_DIR}/02_{year}_{place}.png')

# レイアウト調整
plt.tight_layout()
plt.show()