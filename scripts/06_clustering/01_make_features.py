import os
import sys
from math import radians, sin, cos, sqrt, atan2
import pandas as pd
import numpy as np
from scipy.spatial.distance import pdist
import warnings
import gzip
from geopy.distance import geodesic

warnings.filterwarnings('ignore')

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message
from make_features_func import make_seg_features

message_path = "/home/fukui/workspace/TravelModeEstimation/logs/log_06_clustering.txt"

files = sys.argv[1:]
path_parts = files[0].split("/")
log_message(f"{path_parts}", message_path)

year = path_parts[-2]
place_ = path_parts[-3]
place = "_".join(place_.split("_")[2:])
OUT_DIR = f"/home/data/fukui/processed/06_01_{place}/{year}/"
os.makedirs(OUT_DIR, exist_ok=True)

df_list = []
for file in files:
    with gzip.open(file, 'rt') as f:
        df = pd.read_csv(f, parse_dates=['datetime'])\
                .query("label == 'non-walk'")\
                .assign(
                        latitude=lambda x: np.radians(x["latitude_anonymous"]),
                        longitude=lambda x: np.radians(x["longitude_anonymous"]),
                        )
        # log_message(f"{df.columns}", message_path)
        df_list.append(df)

df_segment = pd.concat(df_list)
# log_message(f"{df_segment.select_dtypes(include='object').describe(include='all')}", message_path)
# 追加: object型のdescribe結果をテキストファイルに保存
obj_desc = df_segment.select_dtypes(include='object').describe(include='all')
num_desc = df_segment.select_dtypes(include='number').describe(include='all')
with open(f"{OUT_DIR}/segment_describe.txt", 'w') as f:
    f.write(obj_desc.to_string())
    f.write("\n")
    f.write(num_desc.to_string())


seg_features = make_seg_features(df_segment)
log_message("done", message_path)
# log_message(f"{seg_features.head()}", message_path)
seg_features.to_csv(f"{OUT_DIR}/seg_features.csv.gz", index=False, compression="gzip")


