import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from scipy.optimize import linear_sum_assignment
from sklearn.metrics import pairwise_distances
import skfuzzy as fuzz
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os
import numpy as np
from scipy.spatial.distance import cdist
from sklearn.decomposition import PCA
import plotly.express as px
import plotly.graph_objs as go
from sklearn.metrics import silhouette_score, silhouette_samples
import matplotlib.cm as cm
sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

message_path = "/home/fukui/workspace/TravelModeEstimation/logs/log_06_clustering.txt"

def data_sepa(df, buffer):
    df["train_stop_proximity_rate"] = df["train_stop_proximity_rate"].fillna(-1.0)
    df["bus_stop_proximity_rate"] = df["bus_stop_proximity_rate"].fillna(-1.0)
    # df_train = df.query(f"is_walk == 0 & buffer_train >= {buffer} & buffer_bus < {buffer}")
    # df_bus = df.query(f"is_walk == 0 & buffer_bus >= {buffer}")
    # df_other = df.query(f"is_walk == 0 & buffer_train < {buffer} & buffer_bus < {buffer}")
    df_train = df.query(f"is_walk == 0 & buffer_train >= {buffer}")
    df_bus = df.query(f"is_walk == 0 & buffer_bus >= {buffer} & buffer_train < {buffer}")
    df_other = df.query(f"is_walk == 0 & buffer_train < {buffer} & buffer_bus < {buffer}")
    # df_train = df.query(f"label == 'non-walk' & buffer_train >= {buffer}")
    # df_bus = df.query(f"label == 'non-walk' & buffer_bus >= {buffer} & buffer_train < {buffer}")
    # df_other = df.query(f"label == 'non-walk' & buffer_train < {buffer} & buffer_bus < {buffer}")
    return df_train, df_bus, df_other

def start_clustering(df, gis_feature_cols, feature_cols, n_clusters, OUT_DIR):
    df_normal = fuzzy_clustering(df, feature_cols, OUT_DIR, "normal", n_clusters)
    df_clean = fuzzy_clustering(df_normal, gis_feature_cols, OUT_DIR, "gis", n_clusters)
    return df_normal, df_clean

def clustering(df_clean, feature_cols, gis, n_clusters):


    # スケーリング
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_clean[feature_cols])
    kmeans = KMeans(n_clusters=n_clusters, random_state=21)
    clusters = kmeans.fit_predict(X_scaled)

    # 結果をデータフレームに追加
    df_clean[f'cluster_{gis}'] = clusters

    cluster_centers = pd.DataFrame(
        scaler.inverse_transform(kmeans.cluster_centers_),
        columns=feature_cols
    )
    # log_message(f"{cluster_centers}", message_path)
    return df_clean

def fuzzy_silhouette_score(X, cntr, u, m=1.5):
    """
    Fuzzy c-meansクラスタリング後のデータに対して
    fuzzyシルエットスコアを計算する関数

    Parameters
    ----------
    X : ndarray of shape (n_samples, n_features)
        元データ
    cntr : ndarray of shape (n_clusters, n_features)
        クラスタの重心
    u : ndarray of shape (n_clusters, n_samples)
        データ点のクラスタ所属度
    m : float
        fuzzifier (fuzzy指数)

    Returns
    -------
    overall_score : float
        データ全体のfuzzyシルエットスコア
    scores : ndarray of shape (n_samples,)
        各サンプルのfuzzyシルエットスコア
    """
    n_clusters, n_samples = u.shape

    # --- 点とクラスタ中心との距離行列 ---
    dist_to_cntr = cdist(X, cntr, metric="euclidean")  # shape: (n_samples, n_clusters)

    # --- 各点の「所属クラスタ内の距離」a(i) と他クラスタとの距離b(i) ---
    a = np.zeros(n_samples)
    b = np.zeros(n_samples)

    for i in range(n_samples):
        # 所属度をfuzzifierで強調
        u_m = u[:, i] ** m  # shape: (n_clusters,)

        # 各クラスタに対して点iの距離を重み付け平均
        intra_distances = (u_m * dist_to_cntr[i]) / np.sum(u_m)
        a[i] = np.sum(intra_distances)

        # 他クラスタの重み付き距離（点iと全クラスタ中心の距離）
        inter_distances = []
        for k in range(n_clusters):
            # 他のクラスタへの重み付き距離
            u_other = np.delete(u[:, i], k)
            cntr_other = np.delete(cntr, k, axis=0)
            d_other = np.delete(dist_to_cntr[i], k)
            weight_other = u_other ** m
            inter_dist = np.sum(weight_other * d_other) / np.sum(weight_other)
            inter_distances.append(inter_dist)

        # 最小の他クラスタ距離
        b[i] = np.min(inter_distances)

    # fuzzyシルエットスコア
    s = (b - a) / np.maximum(a, b)

    # 全体の加重平均スコア
    weights = np.sum(u ** m, axis=0)
    overall_score = np.sum(weights * s) / np.sum(weights)

    return overall_score, s


def fuzzy_clustering(df_clean, cols, OUT_DIR, gis, n_clusters):
    if gis == "normal":
        # 無限大やNaNの処理
        df_all = df_clean.copy()
        df_all[cols] = df_clean[cols].replace([np.inf, -np.inf], np.nan)
        df_all = df_all.dropna(subset=cols)
    else:
        df_all = df_clean.copy()
    # df_target = df_target[df_target["bearing_change_rate"] != 0]
    # log_message(f"{df_all['label'].value_counts()}", message_path)

    # df_target = df_all.query("label == 'non-walk'")
    df_target = df_all.query("is_walk == 0")
    # log_message(f"{len(df_target)}", message_path)

    # スケーリング
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_target[cols])

    # スケール後の列を追加
    for i, col in enumerate(cols):
        df_target[f"{col}_scaled"] = X_scaled[:, i]

    # 転置
    scaled_cols = [f"{col}_scaled" for col in cols]
    X_scaled_T = df_target[scaled_cols].values.T

    # Fuzzy c-means
    cntr, u, u0, d, jm, p, fpc = fuzz.cluster.cmeans(
        X_scaled_T, c=n_clusters, m=2.0, error=0.005, maxiter=1000, init=None, seed=20
    )

    # シルエットスコア
    overall_score = silhouette_score(X_scaled, np.argmax(u, axis=0))
    log_message(f"{gis} Fuzzy Silhouette Score: {overall_score:.3f}", message_path)

    # 曖昧な点を除外
    # max_membership = np.max(u, axis=0)
    # threshold = 0.9
    # mask = max_membership >= threshold
    # X_filtered = X_scaled[mask]
    # u_filtered = u[:, mask]

    # overall_score_filtered = silhouette_score(X_filtered, np.argmax(u_filtered, axis=0))
    # log_message(f"{gis} 曖昧な点を除外した後のfuzzyシルエットスコア: {overall_score_filtered:.4f}", message_path)

    # クラスタラベルを付与
    cluster_labels = np.argmax(u, axis=0)
    df_target[f"fuzzy_cluster_{gis}"] = cluster_labels
    df_target[f"fuzzy_membership_{gis}"] = u.T.tolist()
    df_target[f"max_membership_{gis}"] = df_target[f"fuzzy_membership_{gis}"].apply(max)

    # 6) 元データに統合（walk = -1、フィルタ落ちnon-walkは -2 にして区別）
    clus_col = f"fuzzy_cluster_{gis}"
    memb_col = f"fuzzy_membership_{gis}"
    maxm_col = f"max_membership_{gis}"

    df_all[clus_col] = -1          # まず全行を walk 相当で初期化
    df_all[memb_col] = np.nan
    df_all[maxm_col] = np.nan

    # フィルタに残った non-walk のみ上書き
    df_all.loc[df_target.index, [clus_col, memb_col, maxm_col]] = \
        df_target[[clus_col, memb_col, maxm_col]]
    # 従来の戻り値を維持しつつ、出力用も返す
    return df_all


# RGBAカラーの生成
def rgba_color(hex_color, alpha):
    hex_color = hex_color.lstrip('#')
    r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    return f'rgba({r},{g},{b},{alpha})'

# 最大所属度のヒストグラム
def Maximum_Membership_Degrees(df, OUT_DIR, gis):
    gis_name = gis.split("_")[0]
    mode_name = gis.split("_")[1]
    os.makedirs(f"{OUT_DIR}/{mode_name}", exist_ok=True)
    # df_clean = df.query("label == 'non-walk'")
    df_clean = df.query("is_walk == 0")
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.hist(df_clean[f"max_membership_{gis_name}"], bins=20, color="skyblue", edgecolor="k", alpha=0.7)
    ax.set_title(f"{gis} Distribution of Maximum Membership Degrees")
    ax.set_xlabel("Maximum Membership Degree")
    ax.set_ylabel("Number of Samples")
    ax.grid(True, linestyle="--", alpha=0.5)
    fig.savefig(f'{OUT_DIR}/{mode_name}/{gis_name}_max_membership_histogram.png')


def fuzzy_cluster_3Dplot(df_clean, OUT_DIR):
    # ---- 可視化用の特徴量 ----
    # x_col = "mean_speed_mps"
    x_col = "mean_vel"
    y_col = "bearing_rate_rad"
    z_col = "duration_sec"

    base_colors = [
        '#417038',  # みどり
        '#ba8b40',  # 黄
        '#942343',  # 赤
        '#043c78'   # 紺
    ]

    # ---- 3Dプロット ----
    # ---- Scatter3d Trace ----
    fig = go.Figure()
    for c in range(4):
        mask = df_clean["fuzzy_cluster"] == c
        # クラスタcに属するデータの透明度（max_membershipで指定）
        cluster_opacity = df_clean.loc[mask, "max_membership"]

        # RGBAカラーに変換
        colors_with_alpha = [
            rgba_color(base_colors[c], alpha) for alpha in cluster_opacity
        ]
        fig.add_trace(
            go.Scatter3d(
                x=df_clean[mask][x_col],
                y=df_clean[mask][y_col],
                z=df_clean[mask][z_col],
                mode="markers",
                name=f"Cluster {c+1}",
                marker=dict(
                    size=2.0,
                    color=colors_with_alpha,
                    # opacity=0.6,
                    showscale=False,
                    # line=dict(width=0.5, color='black'),
                ),
            )
        )
    fig.update_scenes(
        # xaxis=dict(title=f"{x_col}(log)", type="log", title_font_size=18, tickfont_size=15),
        xaxis=dict(
                    # title=f"{x_col}(log)", 
                    title=x_col,
                    title_font_size=18, 
                    tickfont_size=13,
                    # type="log",
                    # range=(10**-5, 10**-1),
                    # mirror="allticks",
                    # autorange="reversed"
                   ),
        yaxis=dict(title=f"{y_col}", 
                   title_font_size=18, 
                   tickfont_size=13,
                #    range=(0,5), 
                   mirror="allticks",
                #    type="log",
                   autorange="reversed"
        ),  # y軸logスケール
        zaxis=dict(title=f"{z_col} ", 
                   title_font_size=18,
                   tickfont_size=13,
                   type="log",
         ),
        aspectmode="manual",
        aspectratio=dict(x=1, y=1, z=1),
        # camera=dict(
        #     eye=dict(x=2, y=2, z=1)
        # ), 
    )
    fig.update_layout(
        title='Fuzzy K-means Clustering (Scatter3d, log x-axis)', # htmlなので<br>で改行
        # legend=dict(xanchor='center',
        #             yanchor='bottom',
        #             x=0.85,
        #             y=0.7,
        #             orientation='v',
        #             bgcolor='white', 
        #             bordercolor='black',
        #             borderwidth=0.1,
        #             ),
        # legend_font_size = 15,
        
    )
    
    # ---- Layout（y軸logスケール） ----
    # layout = go.Layout(
    #     scene=dict(s
    #         xaxis=dict(title=x_col),
    #         yaxis=dict(title=y_col, type="log"),  # y軸logスケール
    #         zaxis=dict(title=z_col)
    #     ),
    #     title="Fuzzy K-means Clustering (Scatter3d, log y-axis)"
    # )

    # ---- Figure作成 ----
    # fig = go.Figure(data=[scatter], layout=layout)
    fig.write_html(f"{OUT_DIR}/fuzzy_kmeans_opacity.html")
    return df_clean

def cluster_boxplot(df, feature_cols, OUT_DIR, gis):
    # 特徴量数に応じて行数を自動調整（列数は4固定）
    df_clean = df.query("is_walk == 0")
    # df_clean = df.query("label == 'non-walk'")
    n_features = len(feature_cols)
    ncols = 4
    nrows = int(np.ceil(n_features / ncols)) if n_features > 0 else 1
    gis_name = gis.split("_")[0]
    mode_name = gis.split("_")[1]
    os.makedirs(f"{OUT_DIR}/{mode_name}", exist_ok=True)
    fig, axes = plt.subplots(nrows, ncols, figsize=(4 * ncols, 4 * nrows))
    axes = np.atleast_1d(axes).ravel()

    for i, col in enumerate(feature_cols):
        ax = axes[i]
        sns.boxplot(ax=ax, x=df_clean[f'fuzzy_cluster_{gis_name}'] + 1, y=col, data=df_clean)
        ax.set_title(col)
        if col == "all_distance" or col == "all_time":
            ax.set_yscale("log")

    # 余ったAxesを非表示（または削除）
    for j in range(n_features, nrows * ncols):
        fig.delaxes(axes[j])

    fig.tight_layout()
    fig.savefig(f'{OUT_DIR}/{mode_name}/{gis_name}_cluster_fuzzy_boxplot.png')


def feature_cluster_centroids(df, feature_cols):
    cluster_cols = [f"cluster_{i}" for i in range(5)]
    centroids = {}
    for col in cluster_cols:
        centroids[col] = (
            df.groupby(col)[feature_cols]
            .mean()
            .sort_index()
            .values
        )
    # --- cluster_0 を基準にして他のクラスタを対応付け ---
    aligned_clusters = {}
    aligned_clusters['cluster_0'] = df['cluster_0'].copy()

    for i in range(1, 5):
        # cluster_0 と cluster_i のセントロイド間距離
        dist_matrix = pairwise_distances(centroids['cluster_0'], centroids[f'cluster_{i}'])
        row_ind, col_ind = linear_sum_assignment(dist_matrix)  # ハンガリアン法

        # cluster_{i} のラベルを cluster_0 に対応付け
        mapping = dict(zip(col_ind, row_ind))
        aligned_clusters[f'cluster_{i}'] = df[f'cluster_{i}'].map(mapping)

    # --- データフレームに対応付け後のクラスタ列を追加 ---
    for col in cluster_cols[1:]:
        df[col + '_aligned'] = aligned_clusters[col]

    # --- 各行で何回同じクラスタに分類されたか計算 ---
    aligned_cols = ['cluster_0'] + [col + '_aligned' for col in cluster_cols[1:]]

    # メソッドチェーンで繰り返し回数の集計と割合を一気に計算
    df = df.assign(consistency_count=lambda x: x[aligned_cols].apply(lambda row: row.value_counts().max(), axis=1))

    consistency_summary = (
        df['consistency_count']
        .value_counts()
        .sort_index()
        .reset_index(name='count')
        .rename(columns={'index': 'num_repeats'})
        .assign(percentage=lambda x: x['count'] / len(df) * 100)
    )

    return df, centroids, consistency_summary



def cluster_pairplot(df, feature_cols, OUT_DIR, gis):
    # cluster_0 をカテゴリ型にしておくと色分けが綺麗になる
    df['cluster_0'] = df['cluster_0'].astype(str)
    # feature_cols だけ抽出
    pairplot_df = df[feature_cols + ['cluster_0']]

    # ペアプロット
    sns.pairplot(pairplot_df, hue="cluster_0", palette="tab10", corner=True, plot_kws={'alpha':0.5, 's':10})
    plt.savefig(f'{OUT_DIR}/cluster_pairplot.png')


def map_plot(df, OUT_DIR, gdf_pref, bus_gdf, train_gdf, gis, gis_name):
    mode_label = df[f'mode_label'].unique().tolist()
    n_rows = int(np.ceil(len(mode_label) / 2))
    n_cols = 2
    fig, axes = plt.subplots(figsize=(10, 10), nrows=n_rows, ncols=n_cols, squeeze=False)
    nagasaki_gdf_pref = gdf_pref.query("prefecture == '長崎県'", engine='python')
    for i, mode in enumerate(mode_label):
        df_cluster = df[df[f'mode_label'] == mode]
        # log_message(f"{len(df_cluster)}points", message_path)
        # log_message(f"{mode} {i}", message_path)
        nagasaki_gdf_pref.plot(
            ax=axes[i//2, i%2], 
            color='0.8', 
            # edgecolor='0.5',
            # linewidth=3
            )
        if mode == "train":
            train_gdf.plot(
                ax=axes[i//2, i%2], 
                color='pink', 
                linewidth=1.1, 
                label='Rail Routes'
                )
        if mode == "bus":
            bus_gdf.plot(
                ax=axes[i//2, i%2], 
                color='skyblue', 
                linewidth=1.1, 
                label='Bus Routes'
                )
        df_cluster.plot(
            ax=axes[i//2, i%2], 
            color='0.0', 
            markersize=0.5, 
            alpha=0.1
            )
        
        axes[i//2, i%2].set_title(f'{mode} ver.')
        axes[i//2, i%2].set_axis_off()
        axes[i//2, i%2].set_xlim(540000, 640000)
        axes[i//2, i%2].set_ylim(3600000, 3700000)
        fig.savefig(f'{OUT_DIR}/cluster_map.png')
    

    return df


def cluster_analysis(df, OUT_DIR):
    df["segment_ratio"] = df["segment_count"] / df["segment_count"].sum()
    df["segment_time_ratio"] = df["all_time"] / df["all_time"].sum()
    df["segment_distance_ratio"] = df["all_distance"] / df["all_distance"].sum()
 

    # --- 描画設定（サイズ・フォント・色） ---
    fig, ax = plt.subplots(ncols=3, nrows=1, figsize=(18, 6), constrained_layout=True)

    if 'mode_label' in df.columns:
        labels = df['mode_label'].apply(lambda k: f'Cluster {df["mode_label"]}').tolist()
    # else:
    #     labels = [f'Cluster {i+1}' for i in range(len(df))]

    colors = plt.get_cmap('tab10').colors[:len(labels)]
    textprops = {"fontsize": 12}
    wedgeprops = {"width": 0.6}  # ドーナツ風で見やすく

    # 1) セグメント数比率
    ax[0].pie(
        df['segment_ratio'],
        labels=labels,
        colors=colors,
        autopct='%.1f%%',
        pctdistance=0.75,
        startangle=90,
        textprops=textprops,
        wedgeprops=wedgeprops,
    )
    ax[0].set_title('Cluster Share by Segment Count', fontsize=14)
    ax[0].set_aspect('equal')

    # 2) 時間比率
    ax[1].pie(
        df['segment_time_ratio'],
        labels=labels,
        colors=colors,
        autopct='%.1f%%',
        pctdistance=0.75,
        startangle=90,
        textprops=textprops,
        wedgeprops=wedgeprops,
    )
    ax[1].set_title('Cluster Share by Segment Time', fontsize=14)
    ax[1].set_aspect('equal')

    ax[2].pie(
        df['segment_distance_ratio'],
        labels=labels,
        colors=colors,
        autopct='%.1f%%',
        pctdistance=0.75,
        startangle=90,
        textprops=textprops,
        wedgeprops=wedgeprops,
    )
    ax[2].set_title('Cluster Share by Segment Distance', fontsize=14)
    ax[2].set_aspect('equal')

    # 仕上げ
    fig.suptitle('Cluster Share Overview', fontsize=16)
    fig.savefig(f'{OUT_DIR}/cluster_analysis.png', dpi=200, bbox_inches='tight')


