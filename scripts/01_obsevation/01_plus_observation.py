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

message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_plus_observation.txt"

files = sys.argv[1:]  # 引数でファイルリストを受け取る

_path = os.path.dirname(files[0])
path_parts = _path.split("/")
# 最後の2つの要素を取得
year = path_parts[-1]
place = path_parts[-2]

year_only = year.split("_")[0]

OUT_DIR = f"/home/data/fukui/outputs/figures/01_observation/01_plus/{place}/{year}"
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

df = pd.concat(weekly_records, ignore_index=True)\
        .groupby(by=['hashed_adid'], as_index=False)\
        .agg({'count': 'sum'})

#補累積分布関数
def ccdf(df: pd.DataFrame) -> np.ndarray:
    # print(df.head())
    freq_array = np.array(df['count'].value_counts())
    # print(freq_array)
    p_list = []
    cumsum = 0.0
    s = float(freq_array.sum())
    # print(s)
    for i in range(len(freq_array)):
        if i == 0:
            p_list.append(0)
        else:
            p = freq_array[i-1]/s
            cumsum += p
            p_list.append(cumsum)

    # print(p_list)
    ccdf_array = 1 - np.array(p_list)
    if ccdf_array[0] == 0:
        ccdf_array[0] = 1.0
    return ccdf_array

frequency_count = df['count'].value_counts().sort_index()
ccdf_array = ccdf(df)

fig, ax = plt.subplots(figsize=(10, 10))
ax.scatter(frequency_count.index, ccdf_array, marker='o', color='r', s=5)
ax.set_xlabel('Frequency', fontsize=13)
ax.set_ylabel('Complementary cumulative probability')
ax.set_xscale('log')  # x軸を対数スケールに設定
ax.set_yscale('log')  # y軸を対数スケールに設定
ax.set_xlim(left=10**0)
ax.set_ylim(top=10**0)
ax.grid(True)  # グリッドの表示
ax.set_title('Complementary cumulative probability', fontsize=17)
plt.tight_layout()
plt.savefig(f"{OUT_DIR}/01_plus_ccdf.png")