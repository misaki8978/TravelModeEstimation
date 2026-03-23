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
def plot_ratio_by_user(df, OUT_DIR):
    # log_message(f"{df.columns}", message_path)
    user_counts = (
                    df.groupby(['hashed_adid', 'is_walk'])
                    .size()
                    .unstack(fill_value=0)
                    .reset_index()
                    )
    log_message(f"{user_counts}", message_path)
    # 歩行・非歩行の比率を求める
    user_counts['walk_ratio'] = user_counts[1] / (user_counts[1] + user_counts[0])
    user_counts['non_walk_ratio'] = user_counts[0] / (user_counts[1] + user_counts[0])

    # ソート（オプション：徒歩比率の高い順など）
    user_counts_sorted = user_counts.sort_values('walk_ratio').reset_index(drop=True)
    weekly_user_counts = (
        df.groupby(['hashed_adid', 'week_start'])
        .size()
        .reset_index(name='count_per_week')
    )
    avg_points_per_week = (
        weekly_user_counts.groupby('hashed_adid')['count_per_week']
        .mean()
        .reset_index(name='avg_weekly_segment')
    )

    # user_counts_sorted に結合（walk_ratio順で整列済み）
    merged_df = user_counts_sorted.reset_index().merge(avg_points_per_week, on='hashed_adid')
    
    x = np.arange(len(merged_df))

    # fig, ax1 = plt.subplots(figsize=(10, 6))

    # # 左軸：walk / non-walk 比率（積み上げ棒）
    # ax1.bar(x, merged_df['walk_ratio'], label='Walk Ratio', color='skyblue')
    # ax1.bar(x, merged_df['non_walk_ratio'], bottom=merged_df['walk_ratio'], label='Non-Walk Ratio', color='salmon')
    # ax1.set_ylabel('Ratio', fontsize=17)
    # ax1.set_ylim(0, 1)
    # ax1.set_xlabel('User Index (sorted by walk ratio)', fontsize=17)
    # ax1.tick_params(axis='x', labelsize=16)
    # ax1.tick_params(axis='y', labelsize=16)
    # ax1.legend(loc='upper left', fontsize=17)
    # ax1.grid(True, axis='y')

    # # 右軸：1週間平均データ数（散布図）
    # ax2 = ax1.twinx()
    # ax2.scatter(x, merged_df['avg_weekly_segment'], color='black', label='Avg Weekly Segment', s=20, zorder=3)
    
    # ax2.set_ylabel('Avg Segment / Week', fontsize=17)
    # # 必要に応じて上限を調整
    # # ax2.set_ylim(0, merged_df['avg_weekly_points'].max() * 1.1) 
    # ax2.legend(loc='upper right', fontsize=17)

    ### 軸を左右交換したバージョン
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax2 = ax1.twinx()
    
    # 右軸：walk / non-walk 比率（積み上げ棒）
    ax2.bar(x, merged_df['walk_ratio'], label='Walk Ratio', color='skyblue')
    ax2.bar(x, merged_df['non_walk_ratio'], bottom=merged_df['walk_ratio'], label='Non-Walk Ratio', color='salmon')
    ax2.set_ylabel('Ratio', fontsize=14)
    ax2.set_ylim(0, 1)
    ax2.set_xlabel('User Index (sorted by walk ratio)', fontsize=14)
    ax2.tick_params(axis='x', labelsize=14)
    ax2.tick_params(axis='y', labelsize=14)
    ax2.legend(loc='upper right', fontsize=14)
    ax2.grid(True, axis='y')

    # 左軸：1週間平均データ数（散布図）
    ax1.scatter(x, merged_df['avg_weekly_segment'], color='black', label='Avg Weekly Segment', s=20, zorder=3)
    
    ax1.set_ylabel('Avg Segment / Week', fontsize=14)
    # 必要に応じて上限を調整
    # ax2.set_ylim(0, merged_df['avg_weekly_points'].max() * 1.1) 
    ax1.legend(loc='upper left', fontsize=14) 
    ### 
    ax1.set_zorder(ax2.get_zorder() + 1)
    ax1.patch.set_visible(False) # 背景を透明にしないと後ろの棒グラフが見えなくなります
    # plt.title('Walk/Non-Walk Ratio and Total Points per User', fontsize=17)
    plt.tight_layout()
    plt.savefig(f'{OUT_DIR}/ratio_by_user.png', dpi=1000)


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

def plot_velocity_distribution_mode_bw(seg_normal, OUT_DIR, marker_map, version):
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
            df.groupby([col_bin, f'mode_label_{version}'])
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
    plt.savefig(f'{OUT_DIR}/{version}_velocity_distribution_mode_bw.png', dpi=300)
    plt.close()


def mode_change(df, cluster_col, gis, version):
    if gis == "other":
        df[f'mode_label_{version}'] = df['fuzzy_cluster_normal'].astype(str).map(cluster_col)
    elif gis == "normal":
        df[f'mode_label_{version}'] = df['fuzzy_cluster_normal'].astype(str).map(cluster_col)
        df[f'mode_label_{version}_gis'] = df['fuzzy_cluster_gis'].astype(str).map(cluster_col)
    else:
        df[f'mode_label_{version}'] = df['fuzzy_cluster_gis'].astype(str).map(cluster_col)
    return df




def _best_text_color(color):
    """背景色に対して読みやすい文字色（黒/白）を返す"""
    return "black"
    # r, g, b = mcolors.to_rgb(color)
    # # 相対輝度 (WCAG)
    # L = 0.2126*r + 0.7152*g + 0.0722*b
    # return "black" if L > 0.6 else "white"

import os
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

def plot_multi_band_with_reference(
    df,
    value_cols,
    # version,
    *,
    label_col="mode_label",
    ref_title="パーソントリップ",
    ref_mode_share_ja=None,
    mode_label_map_ja2en=None,
    titles=None,
    pct_label_threshold=0.02,
    figsize=(6, 6),
    savepath=None,
    color_map=None,
    bar_width=0.3,       # 棒の幅
    x_interval=0.5       # ★追加: 棒の中心間の距離（bar_widthより大きくする必要があります）
):

    # mode_label = label_col + "_" + version
    # --- ラベル・データ準備（前回と同じ） ---
    if mode_label_map_ja2en is None:
        mode_label_map_ja2en = {"バス":"bus", "鉄道":"train", "二輪車":"two-wheeler", "徒歩・その他":"walk", "車":"car"}
    mode_label_map_en2ja = {v: k for k, v in mode_label_map_ja2en.items()}

    if ref_mode_share_ja is None:
        ref_mode_share_ja = {"バス":1.3, "鉄道":2.2, "二輪車":8.1, "徒歩・その他":12.7, "車":75.7}

    ref_total = sum(ref_mode_share_ja.values()) or 1.0
    ref_items = []
    ref_prop_dict = {}
    for ja, v in ref_mode_share_ja.items():
        en = mode_label_map_ja2en.get(ja, ja)
        ratio = float(v) / ref_total
        ref_items.append((en, ratio))
        ref_prop_dict[en] = ratio
    ref_items.sort(key=lambda x: x[1], reverse=True)
    ref_order = [lab for lab, _ in ref_items]

    if titles is None: titles = value_cols
    
    plot_data_list = []
    plot_data_list.append({"title": ref_title, "props": ref_prop_dict})

    for col, t in zip(value_cols, titles):
        prop = dict.fromkeys(ref_order, 0.0)

        # sub[col] が Series でも DataFrame でも扱えるようにする
        sub = df[[label_col]].copy()
        col_obj = df[col]

        if isinstance(col_obj, pd.DataFrame):
            # 複数列扱いになっている場合は行方向に合算して1列に潰す
            sub["_value_"] = pd.to_numeric(col_obj.sum(axis=1), errors="coerce")
        else:
            sub["_value_"] = pd.to_numeric(col_obj, errors="coerce")

        total = float(sub["_value_"].sum())

        if total > 0 and np.isfinite(total):
            g = sub.groupby(label_col, as_index=False)["_value_"].sum()
            for _, row in g.iterrows():
                lbl_str = str(row[label_col])
                if lbl_str in prop:
                    prop[lbl_str] = float(row["_value_"]) / total

        plot_data_list.append({"title": t, "props": prop})


    # --- 描画処理 ---
    fig, ax = plt.subplots(1, 1, figsize=figsize, constrained_layout=True)

    # ★変更点: X軸の位置をインターバルに基づいて計算
    # 例: x_interval=0.6 なら [0.0, 0.6, 1.2, ...] となる
    x_positions = [i * x_interval for i in range(len(plot_data_list))]
    
    legend_handles = {}

    for x_idx, data_info in zip(x_positions, plot_data_list):
        props = data_info["props"]
        bottom = 0.0 
        
        for lab in ref_order:
            height = props.get(lab, 0.0)
            if height <= 0: continue
            
            face = (color_map or {}).get(lab, None)
            bar = ax.bar(x_idx, height, width=bar_width, bottom=bottom, 
                         align="center", color=face, edgecolor="white", linewidth=0.5)
            legend_handles[lab] = bar[0]

            # --- 数値ラベルの表示制御 ---
            label_text = f"{height*100:.1f}%"
            
            if height >= pct_label_threshold:
                # しきい値以上なら「棒の中」に表示
                ax.text(x_idx, bottom + height/2, label_text,
                        ha="center", va="center", fontsize=10, fontweight="bold",
                        color=_best_text_color(bar[0].get_facecolor()))
            else:
                # ★追加変更: しきい値未満なら「棒の右外側」に表示
                # x位置: x_idx(棒の中心) + 棒の幅半分 + 少し余白(0.02)
                text_x = x_idx + (bar_width / 2) + 0.02
                text_y = bottom + height / 2
                
                # 色は視認性のため黒(black)を使用し、左揃え(ha="left")で配置
                ax.text(text_x, text_y, label_text,
                        ha="left", va="center", fontsize=9, fontweight="bold",
                        color="black")

            bottom += height

    # --- 軸・凡例設定 ---
    # ★変更点: 計算した x_positions を目盛りの位置として設定
    ax.set_xticks(x_positions)
    ax.set_xticklabels([d["title"] for d in plot_data_list], fontsize=12, rotation=45)
    
    ax.set_ylim(0, 1)
    yticks = [0, 0.25, 0.5, 0.75, 1.0]
    ax.set_yticks(yticks)
    ax.set_yticklabels([f"{int(t*100)}%" for t in yticks], fontsize=9)
    
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.grid(axis='y', linestyle='--', alpha=0.5)

    handles = [legend_handles.get(lab) for lab in reversed(ref_order) if lab in legend_handles]
    labels = [lab for lab in reversed(ref_order) if lab in legend_handles]
    
    if handles:
        ax.legend(handles, labels, loc='upper left', bbox_to_anchor=(1, 1), 
                  title="Transportation Mode", frameon=True)

    if savepath:
        os.makedirs(os.path.dirname(savepath), exist_ok=True)
        plt.savefig(savepath, dpi=220, bbox_inches="tight")

def plot_multi_band_with_reference_version(
    df,
    value_cols,
    version,
    *,
    label_col="mode_label",
    ref_title="パーソントリップ",
    ref_mode_share_ja=None,
    mode_label_map_ja2en=None,
    titles=None,
    pct_label_threshold=0.03,
    figsize=(6, 6),
    savepath=None,
    color_map=None,
    bar_width=0.3,       # 棒の幅
    x_interval=0.5       # ★追加: 棒の中心間の距離（bar_widthより大きくする必要があります）
):

    mode_label = label_col + "_" + version
    # --- ラベル・データ準備（前回と同じ） ---
    if mode_label_map_ja2en is None:
        mode_label_map_ja2en = {"バス":"bus", "鉄道":"train", "二輪車":"two-wheeler", "徒歩・その他":"walk", "車":"car"}
    mode_label_map_en2ja = {v: k for k, v in mode_label_map_ja2en.items()}

    if ref_mode_share_ja is None:
        ref_mode_share_ja = {"バス":1.3, "鉄道":2.2, "二輪車":8.1, "徒歩・その他":12.7, "車":75.7}

    ref_total = sum(ref_mode_share_ja.values()) or 1.0
    ref_items = []
    ref_prop_dict = {}
    for ja, v in ref_mode_share_ja.items():
        en = mode_label_map_ja2en.get(ja, ja)
        ratio = float(v) / ref_total
        ref_items.append((en, ratio))
        ref_prop_dict[en] = ratio
    ref_items.sort(key=lambda x: x[1], reverse=True)
    ref_order = [lab for lab, _ in ref_items]

    if titles is None: titles = value_cols
    
    plot_data_list = []
    plot_data_list.append({"title": ref_title, "props": ref_prop_dict})

    for col, t in zip(value_cols, titles):
        prop = dict.fromkeys(ref_order, 0.0)

        # sub[col] が Series でも DataFrame でも扱えるようにする
        sub = df[[mode_label]].copy()
        col_obj = df[col]

        if isinstance(col_obj, pd.DataFrame):
            # 複数列扱いになっている場合は行方向に合算して1列に潰す
            sub["_value_"] = pd.to_numeric(col_obj.sum(axis=1), errors="coerce")
        else:
            sub["_value_"] = pd.to_numeric(col_obj, errors="coerce")

        total = float(sub["_value_"].sum())

        if total > 0 and np.isfinite(total):
            g = sub.groupby(mode_label, as_index=False)["_value_"].sum()
            for _, row in g.iterrows():
                lbl_str = str(row[mode_label])
                if lbl_str in prop:
                    prop[lbl_str] = float(row["_value_"]) / total

        plot_data_list.append({"title": t, "props": prop})


    # --- 描画処理 ---
    fig, ax = plt.subplots(1, 1, figsize=figsize, constrained_layout=True)

    # ★変更点: X軸の位置をインターバルに基づいて計算
    # 例: x_interval=0.6 なら [0.0, 0.6, 1.2, ...] となる
    x_positions = [i * x_interval for i in range(len(plot_data_list))]
    
    legend_handles = {}

    for x_idx, data_info in zip(x_positions, plot_data_list):
        props = data_info["props"]
        bottom = 0.0 
        
        for lab in ref_order:
            height = props.get(lab, 0.0)
            if height <= 0: continue
            
            face = (color_map or {}).get(lab, None)
            bar = ax.bar(x_idx, height, width=bar_width, bottom=bottom, 
                         align="center", color=face, edgecolor="white", linewidth=0.5)
            legend_handles[lab] = bar[0]

            # --- 数値ラベルの表示制御 ---
            label_text = f"{height*100:.1f}%"
            
            if height >= pct_label_threshold:
                # しきい値以上なら「棒の中」に表示
                ax.text(x_idx, bottom + height/2, label_text,
                        ha="center", va="center", fontsize=10, fontweight="bold",
                        color=_best_text_color(bar[0].get_facecolor()))
            else:
                # ★追加変更: しきい値未満なら「棒の右外側」に表示
                # x位置: x_idx(棒の中心) + 棒の幅半分 + 少し余白(0.02)
                text_x = x_idx + (bar_width / 2) + 0.02
                text_y = bottom + height / 2
                
                # 色は視認性のため黒(black)を使用し、左揃え(ha="left")で配置
                ax.text(text_x, text_y, label_text,
                        ha="left", va="center", fontsize=9, fontweight="bold",
                        color="black")

            bottom += height

    # --- 軸・凡例設定 ---
    # ★変更点: 計算した x_positions を目盛りの位置として設定
    ax.set_xticks(x_positions)
    ax.set_xticklabels([d["title"] for d in plot_data_list], fontsize=9)
    
    ax.set_ylim(0, 1)
    yticks = [0, 0.25, 0.5, 0.75, 1.0]
    ax.set_yticks(yticks)
    ax.set_yticklabels([f"{int(t*100)}%" for t in yticks], fontsize=9)
    
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.grid(axis='y', linestyle='--', alpha=0.5)

    handles = [legend_handles.get(lab) for lab in reversed(ref_order) if lab in legend_handles]
    labels = [lab for lab in reversed(ref_order) if lab in legend_handles]
    
    if handles:
        ax.legend(handles, labels, loc='upper left', bbox_to_anchor=(1, 1), 
                  title="Transportation Mode", frameon=True)

    if savepath:
        os.makedirs(os.path.dirname(savepath), exist_ok=True)
        plt.savefig(savepath, dpi=220, bbox_inches="tight")

# def plot_multi_band_with_reference(
#     df,
#     value_cols,
#     *,
#     label_col="mode_label",
#     ref_title="パーソントリップ",
#     ref_mode_share_ja=None,   # {"バス":1.3,"鉄道":2.2,"自転車":8.1,"徒歩・その他":12.7,"車":75.7}
#     mode_label_map_ja2en=None,
#     titles=None,
#     pct_label_threshold=0.02,
#     figsize=(12, 6.5),
#     savepath=None,
#     color_map=None,
# ):
#     # --- ラベル対応 ---
#     if mode_label_map_ja2en is None:
#         mode_label_map_ja2en = {
#             "バス": "bus",
#             "鉄道": "train",
#             "二輪車": "bicycle",
#             "徒歩・その他": "walk",
#             "車": "car",
#         }
#     # 逆引き（英→日）
#     mode_label_map_en2ja = {v: k for k, v in mode_label_map_ja2en.items()}

#     if ref_mode_share_ja is None:
#         ref_mode_share_ja = {"バス":1.3, "鉄道":2.2, "自転車":8.1, "徒歩・その他":12.7, "車":75.7}

#     # --- PT調査：比率＆描画順（降順） ---
#     ref_total = sum(ref_mode_share_ja.values()) or 1.0
#     ref_items = []
#     for ja, v in ref_mode_share_ja.items():
#         en = mode_label_map_ja2en.get(ja, ja)
#         ref_items.append((en, float(v)/ref_total))
#     ref_items.sort(key=lambda x: x[1], reverse=True)
#     ref_order = [lab for lab, _ in ref_items]  # ← 以降この順

#     # --- 図準備 ---
#     rows = 1 + len(value_cols)
#     if titles is None:
#         titles = value_cols
#     row_titles = [ref_title] + titles

#     fig, axes = plt.subplots(rows, 1, figsize=figsize, sharex=True, constrained_layout=True)
#     if rows == 1:
#         axes = [axes]

#     xticks = [0, 0.25, 0.5, 0.75, 1.0]
#     xtlbls = [f"{int(t*100)}%" for t in xticks]

#     # ---------- 1段目（PT調査） ----------
#     ax0 = axes[0]
#     left = 0.0
#     for lab, w in ref_items:
#         face = (color_map or {}).get(lab, None)
#         bar = ax0.barh(0, w, left=left, align="edge", color=face, height=1.0)
#         if w >= pct_label_threshold:
#             ja = mode_label_map_en2ja.get(lab, lab)
#             ax0.text(left + w/2, 0,
#                      f"{ja}: {w*100:.1f}%",
#                      ha="center", va="top", fontsize=12, fontweight="bold",
#                      color=_best_text_color(bar[0].get_facecolor()))
#         left += w

#     ax0.set_xlim(0, 1); ax0.set_ylim(-0.5, 0.5); ax0.set_yticks([])
#     for s in ("left","right","top","bottom"):
#         ax0.spines[s].set_visible(False)
#     # 行タイトル：帯の“中央”に
#     ax0.text(-0.02, 0.75, row_titles[0], transform=ax0.transAxes,
#              ha="right", va="center", fontsize=13)

#     # ---------- 2段目以降 ----------
#     for ax, value_col, rtitle in zip(axes[1:], value_cols, row_titles[1:]):
#         # ラベル→値の合計→比率
#         prop = dict.fromkeys(ref_order, 0.0)
#         sub = df[[label_col, value_col]].copy()
#         total = float(sub[value_col].sum())
#         if total > 0 and np.isfinite(total):
#             g = sub.groupby(label_col, as_index=False)[value_col].sum()
#             for _, row in g.iterrows():
#                 prop[str(row[label_col])] = float(row[value_col]) / total

#         left = 0.0
#         for lab in ref_order:
#             w = prop.get(lab, 0.0)
#             if w <= 0:
#                 continue
#             face = (color_map or {}).get(lab, None)
#             bar = ax.barh(0, w, left=left, align="edge", color=face, height=1.0)
#             if w >= pct_label_threshold:
#                 ja = mode_label_map_en2ja.get(lab, lab)
#                 ax.text(left + w/2, 0,
#                         f"{ja}: {w*100:.1f}%",
#                         ha="center", va="top", fontsize=12, fontweight="bold",
#                         color=_best_text_color(bar[0].get_facecolor()))
#             left += w

#         ax.set_xlim(0, 1); ax.set_ylim(-0.5, 0.5); ax.set_yticks([])
#         for s in ("left","right","top","bottom"):
#             ax.spines[s].set_visible(False)
#         # 行タイトル：中央に
#         ax.text(-0.02, 0.75, rtitle, transform=ax.transAxes,
#                 ha="right", va="center", fontsize=13)

#     # x 目盛は最下段のみ
#     for ax in axes[:-1]:
#         ax.set_xticks([])
#     axes[-1].set_xticks(xticks)
#     axes[-1].set_xticklabels(xtlbls, fontsize=11)

#     # axes.set_title(value_cols[0], fontsize=13)


#     if savepath:
#         os.makedirs(os.path.dirname(savepath), exist_ok=True)
#         plt.savefig(savepath, dpi=220, bbox_inches="tight")    



def plot_mode_analysis(df, OUT_DIR, color_map, version):
    df = df.reset_index(drop=True)
    fig, axes = plt.subplots(2, 3, figsize=(10, 10))
    # (1) walk / non‑walk の本数（All）
    count = df[f'mode_label_{version}'].value_counts()
    labels_order = count.index.tolist()
    log_message(", ".join(map(str, labels_order)), message_path)
    sns.barplot(
        x=labels_order,
        y=count.values,
        ax=axes[0, 0],
        palette=[color_map.get(lbl, None) for lbl in labels_order]
    )
    axes[0, 0].set_title('Segments Count', fontsize=15)
    axes[0, 0].set_xlabel('Label', fontsize=13)
    axes[0, 0].set_ylabel('Count', fontsize=13)
    axes[0,0].tick_params(axis='x', labelsize=12, rotation=35)

    #(2) 総セグメント時間
    total_duration = df.groupby(f'mode_label_{version}')['all_time'].sum().reindex(labels_order, fill_value=0)
    sns.barplot(
        x=labels_order,
        y=total_duration.values,
        ax=axes[0, 1],
        palette=[color_map.get(lbl, None) for lbl in labels_order]
    )
    axes[0, 1].set_title('Total Duration', fontsize=15)
    axes[0, 1].set_xlabel('Label', fontsize=13)
    axes[0, 1].set_ylabel('Total Duration (sec)', fontsize=13)
    axes[0, 1].tick_params(axis='x', labelsize=12, rotation=35)
    # axes[0, 1].bar_label(barplot.containers[0],  # バーオブジェクト
    #                     labels=[f'{v:.1f}' for v in total_duration.values],
    #                     padding=3)

    #(3)総セグメント距離
    total_distance = df.groupby(f'mode_label_{version}')['all_distance'].sum().reindex(labels_order, fill_value=0)
    sns.barplot(
        x=labels_order,
        y=total_distance.values,
        ax=axes[0, 2],
        palette=[color_map.get(lbl, None) for lbl in labels_order]
    )
    axes[0, 2].set_title('Total Distance', fontsize=15)
    axes[0, 2].set_xlabel('Label', fontsize=13)
    axes[0, 2].set_ylabel('Total Distance (m)', fontsize=13)
    axes[0, 2].tick_params(axis='x', labelsize=12, rotation=35)
    # axes[1, 0].bar_label(barplot.containers[0],  # バーオブジェクト
    #                     labels=[f'{v:.1f}' for v in total_distance.values],
    #                     padding=3)

    # (4) 平均速度 boxplot（All）
    sns.boxplot(
        data=df,
        x=f'mode_label_{version}',
        y='mean_speed',
        ax=axes[1, 0],
        order=labels_order,
        palette=[color_map.get(lbl, None) for lbl in labels_order]
    )
    axes[1, 0].set_title('Mean Velocity', fontsize=15)
    axes[1, 0].set_xlabel('Label', fontsize=13)
    axes[1, 0].set_ylabel('Mean Velocity(m/s)', fontsize=13)
    axes[1, 0].tick_params(axis='x', labelsize=12, rotation=35)

    #(5) buffer_bus
    sns.boxplot(
        data=df,
        x=f'mode_label_{version}',
        y='buffer_bus',
        ax=axes[1, 1],
        order=labels_order,
        palette=[color_map.get(lbl, None) for lbl in labels_order]
    )
    axes[1, 1].set_title('Proximity Rate to Bus Route', fontsize=15)
    axes[1, 1].set_xlabel('Label', fontsize=13)
    axes[1, 1].set_ylabel('Proximity Rate to Bus Route', fontsize=13)
    axes[1, 1].tick_params(axis='x', labelsize=12, rotation=35)
    #(6) buffer_train
    sns.boxplot(
        data=df,
        x=f'mode_label_{version}',
        y='buffer_train',
        ax=axes[1, 2],
        order=labels_order,
        palette=[color_map.get(lbl, None) for lbl in labels_order]
    )
    axes[1, 2].set_title('Proximity Rate to Railway', fontsize=15)
    axes[1, 2].set_xlabel('Label', fontsize=13)
    axes[1, 2].set_ylabel('Proximity Rate to Railway ', fontsize=13)
    axes[1, 2].tick_params(axis='x', labelsize=12, rotation=35)
    plt.tight_layout()
    plt.savefig(f'{OUT_DIR}/{version}_mode_analysis.png', dpi=300)
    plt.close()

def all_analysis(df, OUT_DIR, mapping):
    selected_cols = list(mapping.keys())
    df = df.query("is_walk == 0 & max_speed <= 150")
    target_df = df[selected_cols].rename(columns=mapping)
    plt.figure(figsize=(4, 6))
    sns.boxplot(data=target_df)
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    # plt.ylim(1, 500000)
    # plt.yscale("log")
    # plt.title('Comparison of feature distributions')
    plt.savefig(f'{OUT_DIR}/speed_analysis.png', dpi=300)
    plt.close()
    # return df

def segment_mode(df, version):
    if version == "normal":
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
                                mode_list_normal = (f'mode_label_{version}', 'unique'),
                                mode_list_gis = (f'mode_label_{version}_gis', 'unique'),
                                is_holiday = ('is_holiday', 'first')
                            )\
                            .assign(
                                mode_represent = lambda x: x['mode_list_normal'].apply(mode_represent),
                                mode_represent_gis = lambda x: x['mode_list_gis'].apply(mode_represent)
                            )
    else:
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
                                mode_list = (f'mode_label_{version}', 'unique'),
                                is_holiday = ('is_holiday', 'first')
                            )\
                            .assign(
                                mode_represent = lambda x: x['mode_list'].apply(mode_represent)
                            )
        # 曜日（0=月曜, 6=日曜）


    return df_move, df_mode_represent


def mode_represent(list_mode):
    yusen_mode =["train", "bus", "car", "bike", "two-wheeler", "walk"]
    for i in range(len(yusen_mode)):
        if yusen_mode[i] in list_mode:
            return yusen_mode[i]
    return "walk"


def accuracy_analysis(mode_pred, mode_true):
    mode_pred_100 = mode_pred * 100
    ref_series = pd.Series(mode_true)
    intersection = np.minimum(mode_pred_100, ref_series).sum()

    log_message(f"mode_pred_100: {mode_pred_100}", message_path)
    log_message(f"ref_series: {ref_series}", message_path)
    log_message(f"intersection: {intersection}", message_path)
    return intersection