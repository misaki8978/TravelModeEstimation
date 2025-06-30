import sys
import os
import pandas as pd
import gzip

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

message_path = "/home/fukui/workspace/TravelModeEstimation/logs/log_00_info.txt"

file = sys.argv[1]

log_message(f"{file}", message_path)

with gzip.open(file, 'rt') as f:
    df = pd.read_csv(f, parse_dates=["datetime"])
    log_message(f"{df.select_dtypes(include='object').describe(include='all')}", message_path)
    log_message(f"{df.select_dtypes(include='number').describe(include='all')}", message_path)
    log_message(f"{df.shape[0]} rows", message_path)
    log_message(f"{df.shape[1]} columns", message_path)
    log_message(f"{df.columns.tolist()}", message_path)
    log_message(f"{df.head()}", message_path)
    log_message(f"{df.tail()}", message_path)
    log_message(f"{df['speed'].describe()}", message_path)
    # log_message(f"{df.info()}", message_path)
    f.close()







