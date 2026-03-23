import pandas as pd
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import matplotlib.pyplot as plt
import seaborn as sns
import os
import warnings
import numpy as np
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
# year = path_parts[-1]
# place_ = path_parts[-2]
# place = "_".join(place_.split("_")[2:])
place = "_".join(path_parts[-2].split("_")[-4:-2])
year = "_".join(path_parts[-2].split("_")[-2:])
log_path = f"/home/fukui/workspace/TravelModeEstimation/logs/01_observation/{place}_{year}"
os.makedirs(log_path, exist_ok=True)
message_path = f"{log_path}/02_stay_and_move.txt"
OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/{place}/{year}/02_stay_and_move"
os.makedirs(OUT_DIR, exist_ok=True)


weekly_records = []

for file in files:
    df = pd.read_csv(file)
    weekly_records.append(df)
    # try:
    #     with gzip.open(file, 'rt') as f:
    #         df = pd.read_csv(f)                
    #         # log_message(f"{(df.shape[0])}", message_path)
    #         weekly_records.append(df)
    #         f.close()
    # except FileNotFoundError:
    #     log_message(f"ファイル {file} が見つかりませんでした。", message_path)

combined_stays = pd.concat(weekly_records, ignore_index=True)\
    # .assign(point_count=lambda x: x['move_id'].value_counts(),
# log_message(f"{combined_stays.columns}", message_path)
combined_stays['is_stay'] = combined_stays['stay_id'].apply(lambda x: 1 if x != -1 else 0)
stay = combined_stays[combined_stays['stay_id'] != -1]
move = combined_stays[combined_stays['stay_id'] == -1]
log_message(f"{place}_{year} stay points: {stay.shape[0]}", message_path)
log_message(f"{place}_{year} move points: {move.shape[0]}", message_path)


def plot_ratio_by_user(df, OUT_DIR):
    # log_message(f"{df.columns}", message_path)
    user_counts = (
                    df.groupby(['hashed_adid', 'is_stay'])
                    .size()
                    .unstack(fill_value=0)
                    .reset_index()
                    )
    log_message(f"{user_counts}", message_path)
    # 歩行・非歩行の比率を求める
    user_counts['stay_ratio'] = user_counts[1] / (user_counts[1] + user_counts[0])
    user_counts['move_ratio'] = user_counts[0] / (user_counts[1] + user_counts[0])
    # user_counts['walk_ratio'] = user_counts['walk'] / (user_counts['walk'] + user_counts['non-walk'])
    # user_counts['non_walk_ratio'] = user_counts['non-walk'] / (user_counts['walk'] + user_counts['non-walk'])

    # ソート（オプション：徒歩比率の高い順など）
    user_counts_sorted = user_counts.sort_values('stay_ratio').reset_index(drop=True)
    # # 各ユーザーごとの合計ポイント数
    # point_counts = df.groupby('hashed_adid')['n_points'].sum().reset_index()
    # point_counts.rename(columns={'n_points': 'total_points'}, inplace=True)

    # user_counts_sorted に結合（walk_ratio順で整列済み）
    # merged_df = user_counts_sorted.merge(point_counts, on='hashed_adid')
    
    x = np.arange(len(user_counts_sorted))

    fig, ax1 = plt.subplots(figsize=(10, 6))

    # 左軸：walk / non-walk 比率（積み上げ棒）
    ax1.bar(x, user_counts_sorted['stay_ratio'], label='Stay Ratio', color='skyblue')
    ax1.bar(x, user_counts_sorted['move_ratio'], bottom=user_counts_sorted['stay_ratio'], label='Move Ratio', color='salmon')
    ax1.set_ylabel('Ratio', fontsize=17)
    ax1.set_ylim(0, 1)
    ax1.set_xlabel('User Index (sorted by stay ratio)', fontsize=17)
    ax1.tick_params(axis='x', labelsize=16)
    ax1.tick_params(axis='y', labelsize=16)
    ax1.legend(loc='upper left', fontsize=17)
    ax1.grid(True, axis='y')


    # plt.title('Walk/Non-Walk Ratio and Total Points per User', fontsize=17)
    plt.tight_layout()
    plt.savefig(f'{OUT_DIR}/ratio_by_user.png', dpi=1000)

plot_ratio_by_user(combined_stays, OUT_DIR)