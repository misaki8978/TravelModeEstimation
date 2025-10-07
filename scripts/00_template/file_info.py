import sys
import os
import pandas as pd
import gzip

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

message_path = "/home/fukui/workspace/TravelModeEstimation/logs/log_00_info.txt"

file = sys.argv[1]

log_message(f"{file}", message_path)

df = pd.read_csv(file)
log_message(f"{df.head()}", message_path)
log_message(f"{df["hashed_adid"].nunique()}", message_path)

with gzip.open(file, 'rt') as f:
    df = pd.read_csv(f)
    # log_message(f"{len(df)}", message_path)
    # log_message(f"{df.select_dtypes(include='object').describe(include='all')}", message_path)
    # log_message(f"{df.select_dtypes(include='number').describe(include='all')}", message_path)
    # log_message(f"{df.shape[0]} rows", message_path)
    # log_message(f"{df.shape[1]} columns", message_path)
    # log_message(f"{df.columns.tolist()}", message_path)
    log_message(f"{df["distance_m"].sum()}", message_path)
    # log_message(f"{df.tail()}", message_path)
    # log_message(f"{df['speed'].describe()}", message_path)
    # log_message(f"{df.info()}", message_path)

    # log_message(f"df緯度経度のソート後: {df['latitude_anonymous'].min()} {df['latitude_anonymous'].max()} {df['longitude_anonymous'].min()} {df['longitude_anonymous'].max()}", message_path)
    f.close()







