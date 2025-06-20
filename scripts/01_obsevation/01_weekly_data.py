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

message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_ob_01_weekly_data.txt"

files = sys.argv[1:]  # 引数でファイルリストを受け取る

_path = os.path.dirname(files[0])
path_parts = _path.split("/")
# 最後の2つの要素を取得
year = path_parts[-1]
place = path_parts[-2]

year_only = year.split("_")[0]

OUT_DIR = "/home/data/fukui/outputs/figures/01_observation"
os.makedirs(OUT_DIR, exist_ok=True)


weekly_records = []
for file in files:
    try:
        with gzip.open(file, 'rt') as f:
            df = pd.read_csv(f)\
                   .query("count < 7000")\
            # log_message(f"{(df.shape[0])}", message_path)
            weekly_records.append(df)
            f.close()
    except FileNotFoundError:
        log_message(f"ファイル {file} が見つかりませんでした。", message_path)

df = pd.concat(weekly_records, ignore_index=True)

# 週ごとのアクティブユーザー数
weekly_active = df.filter(items=['week_start', 'hashed_adid'])\
                  .groupby(by=['week_start']).nunique()

weekly_point = df.filter(items=['week_start', 'count'])\
                 .groupby(by=['week_start']).sum()


user_point = df.filter(items=['hashed_adid', 'count'])\
                 .groupby(by=['hashed_adid']).sum()

user_weekcount = df.filter(items=['hashed_adid', 'week_start'])\
                 .groupby(by=['hashed_adid']).nunique()\
                 .sort_values('week_start')\
                 .merge(user_point, on='hashed_adid', how="left")

# log_message(f"weekly_point: {weekly_point.shape[0]}", message_path)
# log_message(f"{(user_weekcount).head()}", message_path)


fig, ax = plt.subplots(2,1, figsize=(16, 18))
# weekly_active.plot(ax=ax)
ax1 = ax[0]

ax1.plot(
        weekly_active.index,
        weekly_active['hashed_adid'],
        marker='o',
        linestyle='-',
        color='blue',
        markersize=5,
        linewidth=2
        )
ax2 = ax1.twinx()

ax2.plot(
        weekly_point.index,
        weekly_point['count'],
        marker='o',
        linestyle='-',
        color='orange',
        markersize=5,
        linewidth=2
        )
# ax.set_title("週ごとのアクティブユーザー数の推移")
# ax.set_xlabel("週")
# ax.set_ylabel("アクティブユーザー数")

ax1.set_title(f"{year_only} count of activeusers and GPS points per week")
ax1.set_xlabel("week")
ax1.set_ylabel("acditveate users")
ax2.set_ylabel("GPS points")


ax1.set_xticks(weekly_active.index[::4])
ax1.set_xticklabels(weekly_active.index[::4], rotation=50)

ax2.set_yscale('log')


ax1.legend(['users'], loc='upper left')
ax2.legend(['GPS points'],loc='upper right')

ax1.grid(axis='both', which='major', color='gray', linestyle='--', linewidth=0.5)


ax_user = ax[1]

ax_user.scatter(
            user_weekcount['week_start'],
            user_weekcount['count'],
            marker='o',
            color='blue'
            )

ax_user.set_title(f"{year_only} weekly count vs GPS points per user")
ax_user.set_xlabel("weekly count")
ax_user.set_ylabel("GPS points")

ax_user.set_yscale('log')

ax_user.grid(axis='both', which='major', color='gray', linestyle='--', linewidth=0.5)
fig.tight_layout()
fig.savefig(f"{OUT_DIR}/01_{year}_{place}.png")
plt.close(fig)