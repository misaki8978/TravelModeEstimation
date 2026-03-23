import os
import pandas as pd
import matplotlib.pyplot as plt
import gzip
import sys
import warnings
import numpy as np
plt.rcParams['font.size'] = 15
# import japanize_matplotlib


sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

warnings.filterwarnings('ignore')



files = sys.argv[1:]  # 引数でファイルリストを受け取る

# df = pd.read_csv(files[0], compression='gzip')
# log_message(f"{len(df)}", message_path)
# log_message(f"{df.columns}", message_path)
# log_message(f"{df[df["bus_stop_proximity_rate"]!=0]["bus_stop_proximity_rate"].describe()}", message_path)
# log_message(f"{df[df["train_stop_proximity_rate"]!=0]["train_stop_proximity_rate"].describe()}", message_path)

# _path = os.path.dirname(files[0])
path_parts = files[0].split("/")
# 最後の2つの要素を取得
place_year = path_parts[-3]

# year_only = year.split("_")[0]
log_path = f"/home/fukui/workspace/TravelModeEstimation/logs/01_observation/{place_year}"
os.makedirs(log_path, exist_ok=True)
message_path = f"{log_path}/01_weekly_data.txt"
OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/01_weekly_data/{place_year}"
os.makedirs(OUT_DIR, exist_ok=True)

for file in files:
    version = file.split("/")[-1].split("_")[:-2]
    version = "_".join(version)
    log_message(f"version: {version}", message_path)
    df = pd.read_csv(file, compression="gzip")
    log_message(f"segment count:{(df.shape[0])}", message_path)
    log_message(f"buffer_train >= 0.7: {df[df['buffer_train']>=0.7].shape[0]}", message_path)
    log_message(f"buffer_bus >= 0.7: {df[df['buffer_bus']>=0.7].shape[0]}", message_path)
    log_message(f"buffer_train >= 0.7 & buffer_bus >= 0.7: {df[(df['buffer_train']>=0.7) & (df['buffer_bus']>=0.7)].shape[0]}", message_path)
    log_message(f"buffer_train >= 0.7 & buffer_bus < 0.7: {df[(df['buffer_train']>=0.7) & (df['buffer_bus']<0.7)].shape[0]}", message_path)
    log_message(f"buffer_train < 0.7 & buffer_bus >= 0.7: {df[(df['buffer_train']<0.7) & (df['buffer_bus']>=0.7)].shape[0]}", message_path)
    log_message(f"buffer_train < 0.7 & buffer_bus < 0.7: {df[(df['buffer_train']<0.7) & (df['buffer_bus']<0.7)].shape[0]}", message_path)
    label_cols = [c for c in df.columns if 'mode_label' in c] 
    for label_col in label_cols:
        log_message(f"{label_col}: {df[label_col].value_counts()}", message_path)


# weekly_records = []
# for file in files:
#     try:
#         with gzip.open(file, 'rt') as f:
#             df = pd.read_csv(f)
#             log_message(f"{(df.shape[0])}", message_path)
#             weekly_records.append(df)
#             f.close()
#     except FileNotFoundError:
#         log_message(f"ファイル {file} が見つかりませんでした。", message_path)

# log_message(f"weekly_records: {len(weekly_records)}", message_path)
# df = pd.concat(weekly_records, ignore_index=True)

# # log_message(f"{df.shape[0]} rows", message_path)
# # # log_message(f"{df.query('count < 7000').shape[0]} rows", message_path)
# # log_message(f"{df['count'].sum()} GPS points", message_path)
# log_message(f"{len(df['hashed_adid'].unique())} users", message_path)
# log_message(f"{len(df)} GPS points", message_path)
# bin_width = 100

# # ビンの区切りを作成 (0から最大値まで1000刻み)
# # 最大値を含むように + bin_width をしています
# # bins = range(0, df['count'].max() + bin_width, bin_width)
# bins = range(0, 15000 + bin_width, bin_width)

# plt.figure(figsize=(10, 6))

# # ヒストグラムの描画
# # bins引数に作成した区切りリストを渡します
# plt.hist(df['count'], bins=bins, edgecolor='black', alpha=0.7)

# # グラフの装飾
# # plt.title('Histogram of Count Frequency (Bin Size = 100)')
# plt.xlabel('Count', fontsize=17)
# plt.ylabel('Frequency', fontsize=17)

# # X軸の目盛りをビンの区切りに合わせて表示（見やすくするため回転）
# plt.xticks(bins, fontsize=15)
# plt.xticks(np.arange(1000, 15000 + 2000, 2000))
# plt.yscale('log')
# plt.grid(axis='y', alpha=0.5)

# # 表示
# plt.tight_layout()
# plt.savefig(f"{OUT_DIR}/01_weekly_data_histogram.png")


# # 週ごとのアクティブユーザー数
# weekly_active = df.filter(items=['week_start', 'hashed_adid'])\
#                   .groupby(by=['week_start']).nunique()

# weekly_point = df.filter(items=['week_start', 'count'])\
#                  .groupby(by=['week_start']).sum()


# user_point = df.filter(items=['hashed_adid', 'count'])\
#                  .groupby(by=['hashed_adid']).sum()

# user_weekcount = df.filter(items=['hashed_adid', 'week_start'])\
#                  .groupby(by=['hashed_adid']).nunique()\
#                  .sort_values('week_start')\
#                  .merge(user_point, on='hashed_adid', how="left")

# # log_message(f"weekly_point: {weekly_point.shape[0]}", message_path)
# # log_message(f"{(user_weekcount).head()}", message_path)


# fig, ax = plt.subplots(2,1, figsize=(16, 18))
# # weekly_active.plot(ax=ax)
# ax1 = ax[0]

# ax1.plot(
#         weekly_active.index,
#         weekly_active['hashed_adid'],
#         marker='o',
#         linestyle='-',
#         color='blue',
#         markersize=5,
#         linewidth=2
#         )
# ax2 = ax1.twinx()

# ax2.plot(
#         weekly_point.index,
#         weekly_point['count'],
#         marker='o',
#         linestyle='-',
#         color='orange',
#         markersize=5,
#         linewidth=2
#         )
# # ax.set_title("週ごとのアクティブユーザー数の推移")
# # ax.set_xlabel("週")
# # ax.set_ylabel("アクティブユーザー数")

# ax1.set_title(f"{year_only} count of activeusers and GPS points per week")
# ax1.set_xlabel("week")
# ax1.set_ylabel("acditveate users")
# ax2.set_ylabel("GPS points")


# ax1.set_xticks(weekly_active.index[::4])
# ax1.set_xticklabels(weekly_active.index[::4], rotation=50)

# ax2.set_yscale('log')


# ax1.legend(['users'], loc='upper left')
# ax2.legend(['GPS points'],loc='upper right')

# ax1.grid(axis='both', which='major', color='gray', linestyle='--', linewidth=0.5)


# ax_user = ax[1]

# ax_user.scatter(
#             user_weekcount['week_start'],
#             user_weekcount['count'],
#             marker='o',
#             color='blue'
#             )

# ax_user.set_title(f"{year_only} Total GPS Points by Number of Active Weeks", fontsize=17)
# ax_user.set_xlabel("Annual Appearance Weeks", fontsize=15)
# ax_user.set_ylabel("Total GPS Points", fontsize=15)

# ax_user.set_yscale('log')

# ax_user.grid(axis='both', which='major', color='gray', linestyle='--', linewidth=0.5)
# fig.tight_layout()
# fig.savefig(f"{OUT_DIR}/weekly_base_plot.png")
# plt.close(fig)