import sys
import os
from geopy.distance import geodesic
import pandas as pd
import plotly.express as px
from scipy.spatial.distance import pdist
import numpy as np
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
from math import radians, sin, cos, sqrt, atan2
import warnings
import gzip
import multiprocessing
from functools import partial

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from segmentation_func import process_all_segments

warnings.filterwarnings('ignore')

# 閾値設定
v = 1.8             # 速度の閾値 (m/s)
a = 0.6             # 加速度の閾値 (m/s^2)
dist1 = 10          # short セグメントと判定する距離 (m)
dist2 = 50         # uncertain/certain 判定用距離 (m)
limit = 4           # uncertain セグメント連続回数の閾値

message_path = "/home/fukui/workspace/TravelModeEstimation/logs/log_05_segment.txt"


file = sys.argv[1]
path_parts = file.split("/")
# log_message(f"{path_parts}", message_path)
# 最後の2つの要素を取得
place = "_".join(path_parts[-3].split("_")[-2:])
year = path_parts[-3]
month = path_parts[-1].split("_")[0]
OUT_DIR = f"/home/data/fukui/processed/05_01_{place}/{year}/"
os.makedirs(OUT_DIR, exist_ok=True)

try:
    # log_message(f"{filename}を処理中...")
    with gzip.open(file, 'rt') as f:
        df = pd.read_csv(f, parse_dates=["datetime"])\
                .assign(
                        latitude_anonymous=lambda x: np.radians(x["latitude_anonymous"]),
                        longitude_anonymous=lambda x: np.radians(x["longitude_anonymous"]),
                        )
        f.close()
except FileNotFoundError:
    log_message(f"ファイル {file} が見つかりませんでした。")


def _process_task(
                task: str,
                df: pd.DataFrame,
                v_thd: float,
                a_thd: float,
                dist_thd1: int,
                dist_thd2: int,
                limit_thd: int,
                ):

    df = df.query("hashed_adid == @task")
    df = process_all_segments(df, v_thd, a_thd, dist_thd1, dist_thd2, limit_thd)
    return df


process_func = partial(
                        _process_task,
                        df=df,
                        v_thd=v,
                        a_thd=a,
                        dist_thd1=dist1,
                        dist_thd2=dist2,
                        limit_thd=limit,
                        )


segmented_list = []
tasks = df["hashed_adid"].unique()
with multiprocessing.Pool(processes=8) as pool:
    results = pool.map(
        process_func, tasks
    )   
for res in results:
    segmented_list.append(res)

log_message(f"{month} done ", message_path)

df_segmented = pd.concat(segmented_list)
df_segmented.to_csv(
    f"{OUT_DIR}/{month}_segmented.csv.gz", index=False, compression="gzip"
)




