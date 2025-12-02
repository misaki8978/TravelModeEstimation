import sys
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import seaborn as sns
import warnings
import geopandas as gpd
import datashader as ds
import datashader.transfer_functions as tf
from datashader.colors import inferno
from shapely.geometry import LineString, box, Point
import contextily as ctx
import shapely
import jpholiday

from datashader.transfer_functions import dynspread
from math import radians, sin, cos, sqrt, atan2
import matplotlib.pyplot as plt

# 日本語フォントを全体設定
plt.rcParams['font.family'] = 'Noto Sans CJK JP'

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

warnings.filterwarnings('ignore')
# plt.rcParams['font.family'] = 'Meiryo'
message_path = f"/home/fukui/workspace/TravelModeEstimation/logs/log_01_segment_analysis.txt"


def calculate_bearing(lat1, lon1, lat2, lon2):
    # ラジアンに変換
    # lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    d_lon = lon2 - lon1
    y = np.sin(d_lon) * np.cos(lat2)
    x = np.cos(lat1) * np.sin(lat2) - np.sin(lat1) * np.cos(lat2) * np.cos(d_lon)
    return np.arctan2(y, x) 

# =============================================
# 1.  セグメント単位統計
# =============================================
def make_seg_features(df_segment):
    seg_features = df_segment.assign(
                                    lat_next = lambda x: x.groupby(['hashed_adid', 'move_id', 'segment_id'])['latitude_anonymous'].shift(1),
                                    lon_next = lambda x: x.groupby(['hashed_adid', 'move_id', 'segment_id'])['longitude_anonymous'].shift(1),
                                    distance = lambda x: x.apply(
                                                                lambda row: haversine_distance(
                                                                                            row['latitude_anonymous'], 
                                                                                            row['longitude_anonymous'], 
                                                                                            row['lat_next'], 
                                                                                            row['lon_next']
                                                                                            ),
                                                                axis='columns'
                                                                ),
                                    bearing = lambda x: x.apply(
                                        lambda row: calculate_bearing(
                                                                        row['latitude_anonymous'], 
                                                                        row['longitude_anonymous'], 
                                                                        row['lat_next'], 
                                                                        row['lon_next']
                                        ) if pd.notnull(row['lat_next']) and pd.notnull(row['lon_next']) else np.nan,
                                        axis=1
                                       ),
                                    )\
                                    .groupby(['hashed_adid', 'move_id', 'segment_id'], as_index=False)\
                                    .agg(
                                        n_points     = ('datetime', 'count'),
                                        mean_vel     = ('speed', 'mean'),
                                        min_vel      = ('speed', 'min'),
                                        max_vel      = ('speed', 'max'),
                                        mean_acc     = ('acceleration', 'mean'),
                                        max_acc      = ('acceleration', 'max'),
                                        total_distance = ('distance', 'sum'),
                                        duration_sec = ('datetime', lambda x: (x.max() - x.min()).total_seconds()),
                                        label        = ('label', 'first'),
                                        bearing_rate_rad = ('bearing', lambda b: np.mean(np.abs(np.diff(b.dropna().values))) if len(b.dropna()) >= 2 else np.nan)
                                    )\
                                    .reset_index()

                                    # .groupby(['hashed_adid', 'move_id', 'segment_id'], as_index=False)\
                                    # .agg(
                                    #     n_points     = ('datetime', 'count'),
                                    #     total_distance = ('distance', 'sum'),
                                    #     duration_sec = ('datetime', lambda x: (x.max() - x.min()).total_seconds()),
                                    #     distance     = ('distance', 'first'),
                                    #     label        = ('label', 'first')
                                    # )\
                                    # .assign(
                                    #     velocity     = lambda x: x['distance'] / x['duration_sec'],
                                    #     acceleration     = lambda x: x['velocity'].diff() / x['duration_sec']
                                    # )\
    # 最高速度30m/s超またはポイント数4未満のセグメントを含むか判定
    seg_features['has_highspeed'] = (seg_features['max_vel'] > 30) | (seg_features['n_points'] < 4)

    # normal / highspeed に分割
    seg_highspeed = seg_features[seg_features['has_highspeed']].copy()
    seg_normal    = seg_features[~seg_features['has_highspeed']].copy()

    return seg_features, seg_normal, seg_highspeed

# =============================================
# 2.  ユーザー単位統計（normal セグメント基準）
# =============================================
def make_user_stats(df_segment, OUT_DIR):

    seg_features, seg_normal, seg_highspeed = make_seg_features(df_segment)

    user_seg_stats = (
        seg_normal
        .groupby('hashed_adid')
        .agg(
            n_gps_points  = ('n_points', 'sum'),
            n_segments   = ('segment_id', 'nunique'),
            avg_seg_len  = ('n_points', 'mean'),
            avg_duration = ('duration_sec', 'mean')
        )
    )
    # =============================================
    # 3.  可視化
    # =============================================
    sns.set_style('whitegrid')

    # -------------------------------------------------
    # 3‑A  全セグメント対象の 2×3 可視化
    # -------------------------------------------------
    fig_all, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig_all.subplots_adjust(hspace=0.35, wspace=0.25)

    # (1) walk / non‑walk の本数（All）
    count = seg_features['label'].value_counts()
    sns.barplot(x=count.index, y=count.values, ax=axes[0, 0])
    axes[0, 0].set_title('Segments by Label (All)')
    axes[0, 0].set_xlabel('Label')
    axes[0, 0].set_ylabel('Count')

    #(2) 総セグメント時間
    total_duration = seg_features.groupby('label')['duration_sec'].sum()
    barplot = sns.barplot(x=total_duration.index, y=total_duration.values, ax=axes[0, 1])
    axes[0, 1].set_title('Total Duration by Label (All)')
    axes[0, 1].set_xlabel('Label')
    axes[0, 1].set_ylabel('Total Duration (sec)')
    axes[0, 1].bar_label(barplot.containers[0],  # バーオブジェクト
                        labels=[f'{v:.1f}' for v in total_duration.values],
                        padding=3)

    # (3) 平均速度 boxplot（All）
    sns.boxplot(data=seg_features, x='label', y='mean_vel', ax=axes[0, 2])
    axes[0, 2].set_title('Mean Velocity by Label (All)')
    axes[0, 2].set_xlabel('Label')
    axes[0, 2].set_ylabel('Mean Velocity(m/s)')

    # (4) GPS points vs Segments per user（normal）
    sns.scatterplot(data=user_seg_stats, x='n_gps_points', y='n_segments', alpha=0.6, ax=axes[1, 0])
    axes[1, 0].set_title('GPS Points vs Segment Count per User')
    axes[1, 0].set_xlabel('GPS Points per User')
    axes[1, 0].set_ylabel('Segments per User (Normal)')

    # (5) Segment Points vs Duration（ラベル色分け）
    sns.scatterplot(data=seg_features, x='n_points', y='duration_sec', hue='label', alpha=0.6, ax=axes[1, 1])
    axes[1, 1].set_title('Segment Points vs Duration (per segment)')
    axes[1, 1].set_xlabel('Number of Points')
    axes[1, 1].set_ylabel('Duration (sec)')

    #(6) Distance vs Duration（ラベル色分け）
    sns.scatterplot(data=seg_features, x='total_distance', y='duration_sec', hue='label', alpha=0.6, ax=axes[1, 2])
    axes[1, 2].set_title('Distance vs Duration (per segment)')
    axes[1, 2].set_xlabel('Distance (m)')
    axes[1, 2].set_ylabel('Duration (sec)')



    fig_all.tight_layout()
    plt.savefig(f'{OUT_DIR}all_seg_stats.png', dpi=300)
    # -------------------------------------------------
    # 3‑B  normal / highspeed 平均速度 boxplot（1行2列）
    # -------------------------------------------------
    fig_speed, ax_speed = plt.subplots(1, 2, figsize=(10, 5))

    sns.boxplot(data=seg_normal, x='label', y='mean_vel', ax=ax_speed[0])
    # ax_speed[0].set_title('Mean Velocity by Label (Normal)', fontsize=18)
    # ax_speed[0].set_xlabel('Label')
    # ax_speed[0].set_ylabel('Mean Velocity', fontsize=18)
    ax_speed[0].set_ylabel('Mean Velocity', fontsize=18)
    ax_speed[0].set_xticks([0, 1])
    ax_speed[0].set_xticklabels(['walk', 'non-walk'], fontsize=18)
    ax_speed[0].set_yticks(range(0, 25, 5))
    ax_speed[0].set_yticklabels(range(0, 25, 5), fontsize=18)

    sns.boxplot(data=seg_highspeed, x='label', y='mean_vel', ax=ax_speed[1])
    ax_speed[1].set_title('Mean Velocity by Label (Highspeed)', fontsize=18)
    ax_speed[1].set_xlabel('Label')
    ax_speed[1].set_ylabel('Mean Velocity', fontsize=18)

    fig_speed.tight_layout()
    plt.savefig(f'{OUT_DIR}speed_boxplot.png', dpi=300)
    # -------------------------------------------------
    # 3‑C  normal セグメントのみ対象の 2×3 可視化
    # -------------------------------------------------
    fig_norm, axes_norm = plt.subplots(2, 3, figsize=(18, 10))
    fig_norm.subplots_adjust(hspace=0.35, wspace=0.25)

    # (1) walk / non‑walk の本数（All）
    count = seg_normal['label'].value_counts()
    sns.barplot(x=count.index, y=count.values, ax=axes_norm[0, 0])
    axes_norm[0, 0].set_title('Segments by Label (less than 30m/s)')
    axes_norm[0, 0].set_xlabel('Label')
    axes_norm[0, 0].set_ylabel('Count')

    #(2) 総セグメント時間
    total_duration = seg_normal.groupby('label')['duration_sec'].sum()
    barplot = sns.barplot(x=total_duration.index, y=total_duration.values, ax=axes_norm[0, 1])
    axes_norm[0, 1].set_title('Total Duration by Label (less than 30m/s)')
    axes_norm[0, 1].set_xlabel('Label')
    axes_norm[0, 1].set_ylabel('Total Duration (sec)')
    axes_norm[0, 1].bar_label(barplot.containers[0],  # バーオブジェクト
                        labels=[f'{v:.1f}' for v in total_duration.values],
                        padding=3)

    # (3) 平均速度 boxplot（All）
    sns.boxplot(data=seg_normal, x='label', y='mean_vel', ax=axes_norm[0, 2])
    axes_norm[0, 2].set_title('Mean Velocity by Label (less than 30m/s)')
    axes_norm[0, 2].set_xlabel('Label')
    axes_norm[0, 2].set_ylabel('Mean Velocity(m/s)')

    # (4) GPS points vs Segments per user（normal）
    sns.scatterplot(data=user_seg_stats, x='n_gps_points', y='n_segments', alpha=0.6, ax=axes_norm[1, 0])
    axes_norm[1, 0].set_title('GPS Points vs Segment Count per User')
    axes_norm[1, 0].set_xlabel('GPS Points per User')
    axes_norm[1, 0].set_ylabel('Segments per User (Normal)')

    # (5) Segment Points vs Duration（ラベル色分け）
    sns.scatterplot(data=seg_normal, x='n_points', y='duration_sec', hue='label', alpha=0.6, ax=axes_norm[1, 1])
    axes_norm[1, 1].set_title('Segment Points vs Duration (per segment)')
    axes_norm[1, 1].set_xlabel('Number of Points')
    axes_norm[1, 1].set_ylabel('Duration (sec)')

    #(6) Distance vs Duration（ラベル色分け）
    sns.scatterplot(data=seg_normal, x='total_distance', y='duration_sec', hue='label', alpha=0.6, ax=axes_norm[1, 2])
    axes_norm[1, 2].set_title('Distance vs Duration (per segment)')
    axes_norm[1, 2].set_xlabel('Distance (m)')
    axes_norm[1, 2].set_ylabel('Duration (sec)')
    plt.tight_layout()
    plt.savefig(f'{OUT_DIR}norm_seg_stats.png', dpi=300)

    return seg_normal



# 凡例とフォントサイズ調整用の設定
def plot_velocity_distribution(seg_normal, OUT_DIR):
    legend_fontsize = 15
    label_fontsize = 14
    tick_fontsize = 12

    # --- ラベルごとの色を決める ---
    color_map = {"walk": "blue", "non-walk": "orange"}  # 追加

    # --- ビンの設定 ---
    mean_vel_edges = np.arange(0, 32, 1)
    max_vel_edges = np.arange(0, 30, 1)
    acc_edges = np.arange(0, 6.3, 0.3)

    def make_bin_column(df, col, edges, bin_name, label_fmt):
        bins = np.concatenate(([-np.inf], edges))
        labels = ['0'] + [label_fmt(e) for e in edges[1:]]
        df[bin_name] = pd.cut(df[col], bins=bins, labels=labels, right=True, include_lowest=True)
        return df

    # 各特徴量のビン列を作成
    seg_normal = make_bin_column(seg_normal, 'mean_vel', mean_vel_edges, 'mean_vel_bin', lambda x: str(int(x)))
    seg_normal = make_bin_column(seg_normal, 'max_vel', max_vel_edges, 'max_vel_bin', lambda x: str(int(x)))
    seg_normal = make_bin_column(seg_normal, 'max_acc', acc_edges, 'max_acc_bin', lambda x: f'{x:.1f}')

    # --- ラベル別にパーセンテージ分布を計算 ---
    def percentage_by_label(df, col_bin):
        return (
            df.groupby([col_bin, 'label'])
            .size()
            .unstack(fill_value=0)
            .apply(lambda x: 100 * x / x.sum(), axis=0)
        )

    mean_vel_pct = percentage_by_label(seg_normal, 'mean_vel_bin')
    max_vel_pct = percentage_by_label(seg_normal, 'max_vel_bin')
    acc_pct = percentage_by_label(seg_normal, 'max_acc_bin')

    # --- 図の描画（縦に3つ） ---
    fig, axes = plt.subplots(3, 1, figsize=(8, 15))

    # 平均速度
    for lbl in mean_vel_pct.columns:
        axes[0].plot(mean_vel_pct.index.astype(float), mean_vel_pct[lbl], marker='o', label=lbl, color=color_map.get(lbl, None))  # ← ここで色を指定)
    axes[0].set_title('Mean Velocity', fontsize=17)
    axes[0].set_ylabel('Percentage (%)', fontsize=17)
    axes[0].set_xlabel('Mean Velocity (m/s)', fontsize=17)
    axes[0].tick_params(axis='both', labelsize=tick_fontsize)
    axes[0].grid(True)
    axes[0].legend(fontsize=legend_fontsize)

    # 最大速度
    for lbl in max_vel_pct.columns:
        axes[1].plot(max_vel_pct.index.astype(float), max_vel_pct[lbl], marker='o', label=lbl, color=color_map.get(lbl, None))  # ← ここで色を指定)
    axes[1].set_title('Max Velocity',fontsize=17)
    axes[1].set_ylabel('Percentage (%)', fontsize=13)
    axes[1].set_xlabel('Max Velocity (m/s)', fontsize=13)
    axes[1].tick_params(axis='both', labelsize=tick_fontsize)
    axes[1].grid(True)
    axes[1].legend(fontsize=legend_fontsize)

    # 平均加速度
    for lbl in acc_pct.columns:
        axes[2].plot(acc_pct.index.astype(float), acc_pct[lbl], marker='o', label=lbl, color=color_map.get(lbl, None))  # ← ここで色を指定)
    axes[2].set_title('Max Acceleration', fontsize=17)
    axes[2].set_ylabel('Percentage (%)', fontsize=13)
    axes[2].set_xlabel('Acceleration (m/s²)', fontsize=13)
    axes[2].tick_params(axis='both', labelsize=tick_fontsize)
    axes[2].grid(True)
    axes[2].legend(fontsize=legend_fontsize)

    plt.tight_layout(rect=(0, 0, 1, 0.96))
    plt.savefig(f'{OUT_DIR}velocity_distribution.png', dpi=300)

# マージキーだけ取り出してタプル化
def make_normal_df(df_segment, seg_normal):
    keys = seg_normal[['hashed_adid', 'move_id', 'segment_id']].drop_duplicates()
    keys_set = set([tuple(x) for x in keys.values])

    # dfから条件に一致する行を抽出
    normal_df = df_segment[df_segment.apply(lambda row: (row['hashed_adid'], row['move_id'], row['segment_id']) in keys_set, axis=1)]
    normal_df = normal_df.assign(
                                weekday=lambda x: x['datetime'].dt.weekday\
                                # .map(lambda d: 1 if d >= 5 else 0)
                                )\
                                .astype({'weekday': 'object'})
    speed_df_hikaku = normal_df.assign(
                                    lat_next = lambda x: x.groupby(['hashed_adid', 'move_id', 'segment_id'])['latitude_anonymous'].shift(1),
                                    lon_next = lambda x: x.groupby(['hashed_adid', 'move_id', 'segment_id'])['longitude_anonymous'].shift(1),
                                    distance = lambda x: x.apply(
                                                                lambda row: haversine_distance(
                                                                                            row['latitude_anonymous'], 
                                                                                            row['longitude_anonymous'], 
                                                                                            row['lat_next'], 
                                                                                            row['lon_next']
                                                                                            ),
                                                                axis='columns'
                                                                )
                                    )\
                                .groupby(by=['hashed_adid', 'move_id', 'segment_id'], as_index=False)\
                                .agg(
                                    label = ('label', 'first'),
                                    start_lat = ('latitude_anonymous', 'first'),
                                    start_lon = ('longitude_anonymous', 'first'),
                                    end_lat = ('latitude_anonymous', 'last'),
                                    end_lon = ('longitude_anonymous', 'last'),
                                    mean_velocity = ('speed', 'mean'),
                                    duration_sec = ('datetime', lambda x : (x.max() - x.min()).total_seconds()),
                                    total_distance = ('distance', 'sum'),
                                    n_points = ('datetime', lambda x: len(x)),
                                    weekday = ('weekday', 'first')
                                )\
                                .assign(
                                    S_mean_velocity = lambda x: x['total_distance'] / x['duration_sec']
                                )

    return normal_df, speed_df_hikaku

def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000.0
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return R * c
# speed_df_hikaku
def plot_speed_comparison(speed_df_hikaku, OUT_DIR):
    # 欠損値の除去 & ソート（見やすさのため）
    plot_df = speed_df_hikaku[['mean_velocity', 'S_mean_velocity']].dropna().reset_index(drop=True)

    fig, ax1 = plt.subplots(figsize=(12, 7))
    ax2 = ax1.twinx()
    ax1.plot(plot_df.index, plot_df['mean_velocity'], label='mean_velocity', color='blue', alpha=0.5)
    ax2.plot(plot_df.index, plot_df['S_mean_velocity'], label='S_mean_velocity', color='orange', alpha=0.5)

    # ラベルなどの設定
    plt.title('Comparison of mean_velocity and S_mean_velocity')

    # ax1.set_xlabel('Index (Sample)')
    ax1.set_ylabel('mean_velocity (m/s)')
    ax2.set_ylabel('S_mean_velocity (m/s)')
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')
    plt.tight_layout()
    plt.savefig(f'{OUT_DIR}speed_comparison.png', dpi=300)




#segmentのlabelをuserごとに比率にして可視化
def plot_ratio_by_user(df, OUT_DIR,):
    # user_counts = (
    #                 df.groupby(['hashed_adid', 'is_walk'])
    #                 .size()
    #                 .unstack(fill_value=0)
    #                 .reset_index()
    #                 )
    user_counts = (
                    df.groupby(['hashed_adid', 'label'])
                    .size()
                    .unstack(fill_value=0)
                    .reset_index()
                    )
    log_message(f"{user_counts['label']}", message_path)
    # 歩行・非歩行の比率を求める
    # user_counts['walk_ratio'] = user_counts['1'] / (user_counts['1'] + user_counts['0'])
    # user_counts['non_walk_ratio'] = user_counts['0'] / (user_counts['1'] + user_counts['0'])
    user_counts['walk_ratio'] = user_counts['walk'] / (user_counts['walk'] + user_counts['non-walk'])
    user_counts['non_walk_ratio'] = user_counts['non-walk'] / (user_counts['walk'] + user_counts['non-walk'])

    # ソート（オプション：徒歩比率の高い順など）
    user_counts_sorted = user_counts.sort_values('walk_ratio').reset_index(drop=True)
    # 各ユーザーごとの合計ポイント数
    point_counts = df.groupby('hashed_adid')['n_points'].sum().reset_index()
    point_counts.rename(columns={'n_points': 'total_points'}, inplace=True)

    # user_counts_sorted に結合（walk_ratio順で整列済み）
    merged_df = user_counts_sorted.merge(point_counts, on='hashed_adid')
    
    x = np.arange(len(merged_df))

    fig, ax1 = plt.subplots(figsize=(14, 6))

    # 左軸：walk / non-walk 比率（積み上げ棒）
    ax1.bar(x, merged_df['walk_ratio'], label='Walk Ratio', color='skyblue')
    ax1.bar(x, merged_df['non_walk_ratio'], bottom=merged_df['walk_ratio'], label='Non-Walk Ratio', color='salmon')
    ax1.set_ylabel('Ratio', fontsize=17)
    ax1.set_ylim(0, 1)
    ax1.set_xlabel('User Index (sorted by walk ratio)', fontsize=17)
    ax1.tick_params(axis='x', labelsize=16)
    ax1.tick_params(axis='y', labelsize=16)
    ax1.legend(loc='upper left', fontsize=17)
    ax1.grid(True, axis='y')

    # 右軸：n_point の合計を折れ線で
    ax2 = ax1.twinx()
    ax2.scatter(x, merged_df['total_points'], label='Total Points', color='black', linestyle='--', marker='o')
    ax2.set_ylabel('Total Points', fontsize=17)
    ax2.tick_params(axis='y', labelsize=16)
    ax2.legend(loc='upper right', fontsize=17)

    # plt.title('Walk/Non-Walk Ratio and Total Points per User', fontsize=17)
    plt.tight_layout()
    plt.savefig(f'{OUT_DIR}/ratio_by_user.png', dpi=300)


def plot_velocity_distribution_mode(seg_normal, OUT_DIR, color_map):
    legend_fontsize = 15
    label_fontsize = 14
    tick_fontsize = 12

    # --- ビンの設定 ---
    mean_vel_edges = np.arange(0, 32, 1)
    max_vel_edges = np.arange(0, 30, 1)
    acc_edges = np.arange(0, 6.3, 0.3)

    def make_bin_column(df, col, edges, bin_name, label_fmt):
        bins = np.concatenate(([-np.inf], edges))
        labels = ['0'] + [label_fmt(e) for e in edges[1:]]
        df[bin_name] = pd.cut(df[col], bins=bins, labels=labels, right=True, include_lowest=True)
        return df

    # 各特徴量のビン列を作成
    seg_normal = make_bin_column(seg_normal, 'mean_speed', mean_vel_edges, 'mean_vel_bin', lambda x: str(int(x)))
    seg_normal = make_bin_column(seg_normal, 'max_speed', max_vel_edges, 'max_vel_bin', lambda x: str(int(x)))
    seg_normal = make_bin_column(seg_normal, 'max_accel', acc_edges, 'max_acc_bin', lambda x: f'{x:.1f}')

    # --- ラベル別にパーセンテージ分布を計算 ---
    def percentage_by_label(df, col_bin):
        return (
            df.groupby([col_bin, 'mode_label'])
            .size()
            .unstack(fill_value=0)
            .apply(lambda x: 100 * x / x.sum(), axis=0)
        )

    mean_vel_pct = percentage_by_label(seg_normal, 'mean_vel_bin')
    max_vel_pct = percentage_by_label(seg_normal, 'max_vel_bin')
    acc_pct = percentage_by_label(seg_normal, 'max_acc_bin')

    # --- 図の描画（縦に3つ） ---
    fig, axes = plt.subplots(3, 1, figsize=(8, 15))

    # 平均速度
    for lbl in mean_vel_pct.columns:
        axes[0].plot(mean_vel_pct.index.astype(float), mean_vel_pct[lbl], marker='o', label=lbl, color=color_map.get(lbl, None))  # ← ここで色を指定)
    axes[0].set_title('Mean Velocity', fontsize=17)
    axes[0].set_ylabel('Percentage (%)', fontsize=17)
    axes[0].set_xlabel('Mean Velocity (m/s)', fontsize=17)
    axes[0].tick_params(axis='both', labelsize=tick_fontsize)
    axes[0].grid(True)
    axes[0].legend(fontsize=legend_fontsize)

    # 最大速度
    for lbl in max_vel_pct.columns:
        axes[1].plot(max_vel_pct.index.astype(float), max_vel_pct[lbl], marker='o', label=lbl, color=color_map.get(lbl, None))  # ← ここで色を指定)
    axes[1].set_title('Max Velocity',fontsize=17)
    axes[1].set_ylabel('Percentage (%)', fontsize=13)
    axes[1].set_xlabel('Max Velocity (m/s)', fontsize=13)
    axes[1].tick_params(axis='both', labelsize=tick_fontsize)
    axes[1].grid(True)
    axes[1].legend(fontsize=legend_fontsize)

    # 平均加速度
    for lbl in acc_pct.columns:
        axes[2].plot(acc_pct.index.astype(float), acc_pct[lbl], marker='o', label=lbl, color=color_map.get(lbl, None))  # ← ここで色を指定)
    axes[2].set_title('Max Acceleration', fontsize=17)
    axes[2].set_ylabel('Percentage (%)', fontsize=13)
    axes[2].set_xlabel('Acceleration (m/s²)', fontsize=13)
    axes[2].tick_params(axis='both', labelsize=tick_fontsize)
    axes[2].grid(True)
    axes[2].legend(fontsize=legend_fontsize)

    plt.tight_layout(rect=(0, 0, 1, 0.96))
    plt.savefig(f'{OUT_DIR}/velocity_distribution_mode.png', dpi=300)

def plot_velocity_distribution_mode_bw(seg_normal, OUT_DIR, marker_map):
    legend_fontsize = 15
    label_fontsize = 14
    tick_fontsize = 12

    # --- ビンの設定 ---
    mean_vel_edges = np.arange(0, 32, 1)
    max_vel_edges = np.arange(0, 30, 1)
    acc_edges = np.arange(0, 6.3, 0.3)

    def make_bin_column(df, col, edges, bin_name, label_fmt):
        bins = np.concatenate(([-np.inf], edges))
        labels = ['0'] + [label_fmt(e) for e in edges[1:]]
        df[bin_name] = pd.cut(df[col], bins=bins, labels=labels, right=True, include_lowest=True)
        return df

    seg_normal = make_bin_column(seg_normal, 'mean_speed', mean_vel_edges, 'mean_vel_bin', lambda x: str(int(x)))
    seg_normal = make_bin_column(seg_normal, 'max_speed', max_vel_edges, 'max_vel_bin', lambda x: str(int(x)))
    seg_normal = make_bin_column(seg_normal, 'max_accel', acc_edges, 'max_acc_bin', lambda x: f'{x:.1f}')

    def percentage_by_label(df, col_bin):
        return (
            df.groupby([col_bin, 'mode_label'])
            .size()
            .unstack(fill_value=0)
            .apply(lambda x: 100 * x / x.sum(), axis=0)
        )

    mean_vel_pct = percentage_by_label(seg_normal, 'mean_vel_bin')
    max_vel_pct = percentage_by_label(seg_normal, 'max_vel_bin')
    acc_pct = percentage_by_label(seg_normal, 'max_acc_bin')

    fig, axes = plt.subplots(3, 1, figsize=(8, 15))
    fig.subplots_adjust(hspace=0.7)

    # 平均速度
    for lbl in mean_vel_pct.columns:
        axes[0].plot(
            mean_vel_pct.index.astype(float), mean_vel_pct[lbl],
            marker=marker_map.get(lbl, 'o'),  # ← マーカー指定
            linestyle='-',                    # 実線
            label=lbl, color='black'          # モノクロなので黒線
        )
    # axes[0].set_title('Mean Velocity', fontsize=17)
    axes[0].set_ylabel('Percentage (%)', fontsize=13)
    axes[0].set_xlabel('Mean Velocity (m/s)', fontsize=13)
    axes[0].tick_params(axis='both', labelsize=tick_fontsize)
    axes[0].grid(True)
    axes[0].legend(fontsize=legend_fontsize)

    # 最大速度
    for lbl in max_vel_pct.columns:
        axes[1].plot(
            max_vel_pct.index.astype(float), max_vel_pct[lbl],
            marker=marker_map.get(lbl, 'o'),
            linestyle='-',
            label=lbl, color='black'
        )
    # axes[1].set_title('Max Velocity', fontsize=17)
    axes[1].set_ylabel('Percentage (%)', fontsize=13)
    axes[1].set_xlabel('Max Velocity (m/s)', fontsize=13)
    axes[1].tick_params(axis='both', labelsize=tick_fontsize)
    axes[1].grid(True)
    axes[1].legend(fontsize=legend_fontsize)

    # 最大加速度
    for lbl in acc_pct.columns:
        axes[2].plot(
            acc_pct.index.astype(float), acc_pct[lbl],
            marker=marker_map.get(lbl, 'o'),
            linestyle='-',
            label=lbl, color='black'
        )
    # axes[2].set_title('Max Acceleration', fontsize=17)
    axes[2].set_ylabel('Percentage (%)', fontsize=13)
    axes[2].set_xlabel('Max Acceleration (m/s²)', fontsize=13)
    axes[2].tick_params(axis='both', labelsize=tick_fontsize)
    axes[2].grid(True)
    axes[2].legend(fontsize=legend_fontsize)

    plt.tight_layout(rect=(0, 0, 1, 0.96))
    plt.savefig(f'{OUT_DIR}/velocity_distribution_mode_bw.png', dpi=300)
    plt.close()


def mode_change(df, cluster_col, gis):
    if gis == "other":
        df['mode_label'] = df['fuzzy_cluster_normal'].astype(str).map(cluster_col)
    else:
        df['mode_label'] = df['fuzzy_cluster_gis'].astype(str).map(cluster_col)
    return df




def _best_text_color(color):
    """背景色に対して読みやすい文字色（黒/白）を返す"""
    return "black"
    # r, g, b = mcolors.to_rgb(color)
    # # 相対輝度 (WCAG)
    # L = 0.2126*r + 0.7152*g + 0.0722*b
    # return "black" if L > 0.6 else "white"

def plot_multi_band_with_reference(
    df,
    value_cols,
    *,
    label_col="mode_label",
    ref_title="パーソントリップ",
    ref_mode_share_ja=None,   # {"バス":1.3,"鉄道":2.2,"自転車":8.1,"徒歩・その他":12.7,"車":75.7}
    mode_label_map_ja2en=None,
    titles=None,
    pct_label_threshold=0.02,
    figsize=(12, 6.5),
    savepath=None,
    color_map=None,
):
    # --- ラベル対応 ---
    if mode_label_map_ja2en is None:
        mode_label_map_ja2en = {
            "バス": "bus",
            "鉄道": "train",
            "自転車": "bicycle",
            "徒歩・その他": "walk",
            "車": "car",
        }
    # 逆引き（英→日）
    mode_label_map_en2ja = {v: k for k, v in mode_label_map_ja2en.items()}

    if ref_mode_share_ja is None:
        ref_mode_share_ja = {"バス":1.3, "鉄道":2.2, "自転車":8.1, "徒歩・その他":12.7, "車":75.7}

    # --- PT調査：比率＆描画順（降順） ---
    ref_total = sum(ref_mode_share_ja.values()) or 1.0
    ref_items = []
    for ja, v in ref_mode_share_ja.items():
        en = mode_label_map_ja2en.get(ja, ja)
        ref_items.append((en, float(v)/ref_total))
    ref_items.sort(key=lambda x: x[1], reverse=True)
    ref_order = [lab for lab, _ in ref_items]  # ← 以降この順

    # --- 図準備 ---
    rows = 1 + len(value_cols)
    if titles is None:
        titles = value_cols
    row_titles = [ref_title] + titles

    fig, axes = plt.subplots(rows, 1, figsize=figsize, sharex=True, constrained_layout=True)
    if rows == 1:
        axes = [axes]

    xticks = [0, 0.25, 0.5, 0.75, 1.0]
    xtlbls = [f"{int(t*100)}%" for t in xticks]

    # ---------- 1段目（PT調査） ----------
    ax0 = axes[0]
    left = 0.0
    for lab, w in ref_items:
        face = (color_map or {}).get(lab, None)
        bar = ax0.barh(0, w, left=left, align="edge", color=face, height=1.0)
        if w >= pct_label_threshold:
            ja = mode_label_map_en2ja.get(lab, lab)
            ax0.text(left + w/2, 0,
                     f"{ja}: {w*100:.1f}%",
                     ha="center", va="top", fontsize=12, fontweight="bold",
                     color=_best_text_color(bar[0].get_facecolor()))
        left += w

    ax0.set_xlim(0, 1); ax0.set_ylim(-0.5, 0.5); ax0.set_yticks([])
    for s in ("left","right","top","bottom"):
        ax0.spines[s].set_visible(False)
    # 行タイトル：帯の“中央”に
    ax0.text(-0.02, 0.75, row_titles[0], transform=ax0.transAxes,
             ha="right", va="center", fontsize=13)

    # ---------- 2段目以降 ----------
    for ax, value_col, rtitle in zip(axes[1:], value_cols, row_titles[1:]):
        # ラベル→値の合計→比率
        prop = dict.fromkeys(ref_order, 0.0)
        sub = df[[label_col, value_col]].copy()
        total = float(sub[value_col].sum())
        if total > 0 and np.isfinite(total):
            g = sub.groupby(label_col, as_index=False)[value_col].sum()
            for _, row in g.iterrows():
                prop[str(row[label_col])] = float(row[value_col]) / total

        left = 0.0
        for lab in ref_order:
            w = prop.get(lab, 0.0)
            if w <= 0:
                continue
            face = (color_map or {}).get(lab, None)
            bar = ax.barh(0, w, left=left, align="edge", color=face, height=1.0)
            if w >= pct_label_threshold:
                ja = mode_label_map_en2ja.get(lab, lab)
                ax.text(left + w/2, 0,
                        f"{ja}: {w*100:.1f}%",
                        ha="center", va="top", fontsize=12, fontweight="bold",
                        color=_best_text_color(bar[0].get_facecolor()))
            left += w

        ax.set_xlim(0, 1); ax.set_ylim(-0.5, 0.5); ax.set_yticks([])
        for s in ("left","right","top","bottom"):
            ax.spines[s].set_visible(False)
        # 行タイトル：中央に
        ax.text(-0.02, 0.75, rtitle, transform=ax.transAxes,
                ha="right", va="center", fontsize=13)

    # x 目盛は最下段のみ
    for ax in axes[:-1]:
        ax.set_xticks([])
    axes[-1].set_xticks(xticks)
    axes[-1].set_xticklabels(xtlbls, fontsize=11)

    # axes.set_title(value_cols[0], fontsize=13)


    if savepath:
        os.makedirs(os.path.dirname(savepath), exist_ok=True)
        plt.savefig(savepath, dpi=220, bbox_inches="tight")
    



def plot_mode_analysis(df, OUT_DIR, color_map):
    fig, axes = plt.subplots(2, 2, figsize=(10, 10))
    # (1) walk / non‑walk の本数（All）
    count = df['mode_label'].value_counts()
    labels_order = count.index.tolist()
    log_message(", ".join(map(str, labels_order)), message_path)
    sns.barplot(
        x=labels_order,
        y=count.values,
        ax=axes[0, 0],
        palette=[color_map.get(lbl, None) for lbl in labels_order]
    )
    axes[0, 0].set_title('Segments by Label (All)')
    axes[0, 0].set_xlabel('Label')
    axes[0, 0].set_ylabel('Count')

    #(2) 総セグメント時間
    total_duration = df.groupby('mode_label')['all_time'].sum().reindex(labels_order, fill_value=0)
    sns.barplot(
        x=labels_order,
        y=total_duration.values,
        ax=axes[0, 1],
        palette=[color_map.get(lbl, None) for lbl in labels_order]
    )
    axes[0, 1].set_title('Total Duration by Label (All)')
    axes[0, 1].set_xlabel('Label')
    axes[0, 1].set_ylabel('Total Duration (sec)')
    # axes[0, 1].bar_label(barplot.containers[0],  # バーオブジェクト
    #                     labels=[f'{v:.1f}' for v in total_duration.values],
    #                     padding=3)

    #(3)総セグメント距離
    total_distance = df.groupby('mode_label')['all_distance'].sum().reindex(labels_order, fill_value=0)
    sns.barplot(
        x=labels_order,
        y=total_distance.values,
        ax=axes[1, 0],
        palette=[color_map.get(lbl, None) for lbl in labels_order]
    )
    axes[1, 0].set_title('Total Distance by Label (All)')
    axes[1, 0].set_xlabel('Label')
    axes[1, 0].set_ylabel('Total Distance (m)')
    # axes[1, 0].bar_label(barplot.containers[0],  # バーオブジェクト
    #                     labels=[f'{v:.1f}' for v in total_distance.values],
    #                     padding=3)

    # (4) 平均速度 boxplot（All）
    sns.boxplot(
        data=df,
        x='mode_label',
        y='mean_speed',
        ax=axes[1, 1],
        order=labels_order,
        palette=[color_map.get(lbl, None) for lbl in labels_order]
    )
    axes[1, 1].set_title('Mean Velocity by Label (All)')
    axes[1, 1].set_xlabel('Label')
    axes[1, 1].set_ylabel('Mean Velocity(m/s)')
    plt.tight_layout()
    plt.savefig(f'{OUT_DIR}/mode_analysis.png', dpi=300)
    plt.close()



def segment_mode(df):
    df["date_only"] = df["date"].str[:10]
    df["date_only"] = pd.to_datetime(df["date_only"])
    df["weekday"] = df["date_only"].dt.weekday

    # 土日 or 祝日で休日フラグを立てる
    df["is_holiday"] = (
        (df["weekday"] >= 5) |  # 土日
        (df["date_only"].apply(jpholiday.is_holiday))  # 祝日
    )
    df_move = df.groupby(["hashed_adid", "date_only"])\
                     .agg(
                        n_segments = ('move_id', 'nunique'),
                        is_holiday = ('is_holiday', 'first')
                     )
    df_mode_represent = df.groupby(["hashed_adid", "date_only", "move_id"])\
                          .agg(
                            start_date = ('date', 'first'),
                            end_date = ('date_max', 'last'),
                            mode_list = ('mode_label', 'unique'),
                            is_holiday = ('is_holiday', 'first')
                          )\
                          .assign(
                            mode_represent = lambda x: x['mode_list'].apply(mode_represent)
                          )
    # 曜日（0=月曜, 6=日曜）


    return df_move, df_mode_represent


def mode_represent(list_mode):
    yusen_mode =["train", "bus", "car", "bike", "bicycle", "walk"]
    for i in range(len(yusen_mode)):
        if yusen_mode[i] in list_mode:
            return yusen_mode[i]
    return "walk"