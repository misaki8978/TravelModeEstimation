#! /usr/bin/env python3
import os
import warnings
import pandas as pd
import sys
import gzip
import osmnx as ox
from japanmap import get_data, pref_points, pref_names
from shapely.geometry import Polygon
import geopandas as gpd
from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
warnings.filterwarnings('ignore')
from log_message import log_message


mode_segment_file = sys.argv[1:-1]
true_file = sys.argv[-1]
path_parts = true_file.split("/")
place = path_parts[-3]
year = path_parts[-2]
OUT_DIR = f"/home/data/fukui/processed/010_accuracy/{place}/{year}"
LOG_DIR = f"/home/fukui/workspace/TravelModeEstimation/logs/010_accuracy/{place}/{year}"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)
log_path = f"{LOG_DIR}/accuracy.txt"


TARGET_LABELS = ['two-wheeler', 'car', 'bus', 'train']
true_df = pd.read_csv(true_file).drop(columns=["Note", "mode_label"])
replace_map = {
    'bicycle': 'two-wheeler',
    'Bicycle': 'two-wheeler',
    'Two_wheels': 'two-wheeler' # 念のため
}

# (1) True_Label を置換
true_df['True_Label'] = true_df['True_Label'].replace(replace_map)

exclude_labels = ['walk', 'nan', '']

# True_Labelを小文字にしたものが、除外リストに含まれて「いない(~)」行だけを残す
# これで 'Walk', 'walk', 'NaN', 'nan', 空白 がすべて消えます
true_df = true_df[~true_df['True_Label'].str.lower().isin(exclude_labels)].copy()

# TARGET_LABELSに含まれる行だけを抽出
true_df = true_df[true_df['True_Label'].isin(TARGET_LABELS)].copy()

for mode_segment_file in mode_segment_file:
    mode_segment_df = pd.read_csv(mode_segment_file, compression="gzip")
    
    # log_message(f"mode_segment_df: {mode_segment_df.columns}", log_path)
    mode_segment_df["unique_key"] = mode_segment_df["hashed_adid"].astype(str) + "_" + mode_segment_df["segment_month_id"].astype(str)
    # mode_label = mode_segment_df[mode_segment_df["unique_key"] == "edddbfe412b061e9299b9b3d48c2fdf9_2019-09_8_4"]["mode_label"]
    # log_message(f"mode_label: {mode_label}", log_path)
    label_cols = [c for c in mode_segment_df.columns if 'mode_label' in c]
    true_df = true_df.merge(mode_segment_df[['unique_key', *label_cols]], on="unique_key", how="left")
    # log_message(f"true_df: {true_df.columns}", log_path)
    

true_df.to_csv(f"{OUT_DIR}/compare_true_label.csv", index=False)

# 【修正1】読み込み直後に、NaN(空白)を空文字に変換してしまう
true_df = true_df.fillna('')

# 比較対象カラムの抽出
pred_cols = [c for c in true_df.columns if 'mode_label_' in c]

for pred_col in pred_cols:
    version_name = pred_col.replace('mode_label_', '')
    
    y_true = true_df['True_Label']
    y_pred = true_df[pred_col].astype(str).str.strip()
    
    # ラベルの整合性を確保
    unique_labels = sorted(list(set(y_true.unique()) | set(y_pred.unique())))
    labels = [l for l in TARGET_LABELS if l in unique_labels] + [l for l in unique_labels if l not in TARGET_LABELS]

    # --- A. 混同行列の計算 ---
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    
    # --- B. 画像(PNG)として保存 ---
    plt.figure(figsize=(8, 6))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=labels, yticklabels=labels,annot_kws={'fontsize': 15})
    # sns.set(font_scale=1.5)
    plt.xlabel('Predicted Label', fontsize=13)
    plt.ylabel('True Label', fontsize=13)
    plt.title(f'Confusion Matrix: {version_name}')
    img_path = os.path.join(OUT_DIR, f'cm_{version_name}.png')
    plt.savefig(img_path, bbox_inches='tight')
    plt.close() # 画面には表示せずメモリを解放
    
    # --- C. 数値(CSV)として保存 ---
    # 論文の表にする際にコピペしやすくなります
    csv_path = os.path.join(OUT_DIR, f'cm_{version_name}.csv')
    cm_df = pd.DataFrame(cm, index=labels, columns=labels)
    cm_df.to_csv(csv_path)

    log_message(f"保存完了: {version_name} -> {img_path}, {csv_path}", log_path)

