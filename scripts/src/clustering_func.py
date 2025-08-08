import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from scipy.optimize import linear_sum_assignment
from sklearn.metrics import pairwise_distances
import skfuzzy as fuzz
import matplotlib.pyplot as plt
import seaborn as sns
import sys
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

def clustering(df_clean, feature_cols, i):


    # スケーリング
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_clean[feature_cols])
    random_state = [42, 10, 21, 13, 36]
    kmeans = KMeans(n_clusters=4, random_state=random_state[i])
    clusters = kmeans.fit_predict(X_scaled)

    # 結果をデータフレームに追加
    df_clean[f'cluster_{i}'] = clusters

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


def fuzzy_clustering(df_clean, feature_cols, OUT_DIR):
    log_message(f'{df_clean[feature_cols].corr()}', message_path)
    # 追加: 無限大をNaNに置換し、欠損行を削除
    df_clean[feature_cols] = df_clean[feature_cols].replace([np.inf, -np.inf], np.nan)
    df_clean = df_clean.dropna(subset=feature_cols)
    #スケーリング
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(df_clean[feature_cols])
    # スケール後の列を「_scaled」付きで追加
    for i, col in enumerate(feature_cols):
        df_clean[f"{col}_scaled"] = X_scaled[:, i]
    
    # 転置（skfuzzy は shape=(features, samples) を要求するため）
    scaled_cols = [f"{col}_scaled" for col in feature_cols]
    X_scaled_T = df_clean[scaled_cols].values.T
    # Fuzzy k-means (cmeans) クラスタリング
    n_clusters = 4
    cntr, u, u0, d, jm, p, fpc = fuzz.cluster.cmeans(
        X_scaled_T, c=n_clusters, m=1.5, error=0.005, maxiter=1000, init=None, seed=0
    )

    # シルエットスコアを計算
    # overall_score, s = fuzzy_silhouette_score(X_scaled, cntr, u)
    overall_score = silhouette_score(X_scaled, np.argmax(u, axis=0))
    log_message(f"Fuzzy Silhouette Score: {overall_score:.3f}", message_path)

    # しきい値を決定（例: 0.7未満の点を除外）
    # 最大所属度を計算
    max_membership = np.max(u, axis=0)

    # しきい値を決定（例: 0.7未満の点を除外）
    threshold = 0.9
    mask = max_membership >= threshold

    # 曖昧な点を除外したデータと所属度
    X_filtered = X_scaled[mask]        # 元データ
    u_filtered = u[:, mask]            # 所属度
    log_message(f"フィルタ後のデータ数: {X_filtered.shape[0]} / {X_scaled.shape[0]}", message_path)

    # Fuzzyシルエットスコア再計算
    # overall_score_filtered, _ = fuzzy_silhouette_score(X_filtered, cntr, u_filtered, m=1.5)
    overall_score_filtered = silhouette_score(X_filtered, np.argmax(u_filtered, axis=0))

    log_message(f"曖昧な点を除外した後のfuzzyシルエットスコア: {overall_score_filtered:.4f}", message_path)

    cluster_labels = np.argmax(u, axis=0)

    # 元のデータフレームにクラスタラベルを追加
    df_clean['fuzzy_cluster'] = cluster_labels
    df_clean['fuzzy_membership'] = u.T.tolist()  # 各クラスタへの所属度も保存
    df_clean["max_membership"] = df_clean["fuzzy_membership"].apply(max)

    # クラスタごとの中心値
    cluster_centers = scaler.inverse_transform(cntr)
    log_message(f"{cluster_centers}", message_path)
    # log_message(f"{df_clean.head()}", message_path)


    # u: (c, N)
    N = u.shape[1]
    PC = np.sum(u**2) / N
    PE = -np.sum(u * np.log(u + 1e-12)) / N  # log(0)防止に微小値を足す
    log_message(f"Partition Coefficient: {PC:.4f}", message_path)
    log_message(f"Partition Entropy:     {PE:.4f}", message_path)

    
    # # 次元圧縮（2D）
    # pca = PCA(n_components=2)
    # X_pca = pca.fit_transform(X_scaled)

    # カラーマップ設定
    colors = ['#ff0000', '#7cfc00', '#ffa500', '#00bfff']
    # x, y 軸のデータ
    x = df_clean['duration_sec']
    # y = df_clean['bearing_rate_rad']
    # y = df_clean['mean_speed_mps']
    # cluster_labels = np.argmax(u, axis=0)  # 最大所属度のクラスタ番号（サンプル毎）
    max_membership = np.max(u, axis=0)     # 最大所属度の値（サンプル毎）

    # ヒストグラムを描画
    plt.figure(figsize=(8, 5))
    plt.hist(max_membership, bins=20, color="skyblue", edgecolor="k", alpha=0.7)
    plt.title("Distribution of Maximum Membership Degrees")
    plt.xlabel("Maximum Membership Degree")
    plt.ylabel("Number of Samples")
    plt.grid(True, linestyle="--", alpha=0.5)
    plt.savefig(f'{OUT_DIR}/max_membership_histogram.png')


    # 2) 全体のシルエット係数
    score = silhouette_score(X_scaled, cluster_labels)
    log_message(f"Silhouette Coefficient: {score:.3f}", message_path)

    # 3) サンプルごとのシルエット値を計算してプロット
    sil_vals = silhouette_samples(X_scaled, cluster_labels)
    n_clusters = u.shape[0]
    # 4) シルエットスコアのプロット
    fig, ax = plt.subplots(figsize=(6, 4))
    y_lower = 10
    for i in range(n_clusters):
        cluster_sil_vals = sil_vals[cluster_labels == i]
        cluster_sil_vals.sort()
        size_cluster = cluster_sil_vals.shape[0]
        y_upper = y_lower + size_cluster

        color = cm.nipy_spectral(float(i) / n_clusters)
        ax.fill_betweenx(np.arange(y_lower, y_upper),
                        0, cluster_sil_vals,
                        facecolor=color, edgecolor=color, alpha=0.7)
        ax.text(-0.05, y_lower + 0.5 * size_cluster, str(i))
        y_lower = y_upper + 10  # 次クラスのオフセット

    ax.set_title("Silhouette Plot for Fuzzy C-Means (crisp labels)")
    ax.set_xlabel("Silhouette coefficient values")
    ax.set_ylabel("Cluster label")
    ax.axvline(x=score, color="red", linestyle="--")
    plt.tight_layout()
    fig.savefig(f'{OUT_DIR}/silhouette_plot.png')

    # --- 4. 次元削減（PCA） ---
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)

    # --- 5. 可視化 ---
    plt.figure(figsize=(8,6))
    for i in range(n_clusters):
        plt.scatter(
            X_pca[cluster_labels == i, 0],
            X_pca[cluster_labels == i, 1],
            label=f'Cluster {i}', alpha=0.6)

    plt.title('Fuzzy c-means clustering (PCA 2D)')
    plt.xlabel('PC1')
    plt.ylabel('PC2')
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{OUT_DIR}/pca_plot.png')



    return df_clean

# RGBAカラーの生成
def rgba_color(hex_color, alpha):
    hex_color = hex_color.lstrip('#')
    r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    return f'rgba({r},{g},{b},{alpha})'




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

def cluster_boxplot(df_clean, feature_cols, OUT_DIR):
    plt.figure(figsize=(16, 8))
    for i, col in enumerate(feature_cols):
        plt.subplot(2, 4, i+1)
        sns.boxplot(x=df_clean['fuzzy_cluster'] + 1, y=col, data=df_clean)
        plt.title(col)
        plt.tight_layout()
    plt.savefig(f'{OUT_DIR}/cluster_fuzzy_boxplot.png')


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



def cluster_pairplot(df, feature_cols, OUT_DIR):
    # cluster_0 をカテゴリ型にしておくと色分けが綺麗になる
    df['cluster_0'] = df['cluster_0'].astype(str)
    # feature_cols だけ抽出
    pairplot_df = df[feature_cols + ['cluster_0']]

    # ペアプロット
    sns.pairplot(pairplot_df, hue="cluster_0", palette="tab10", corner=True, plot_kws={'alpha':0.5, 's':10})
    plt.savefig(f'{OUT_DIR}/cluster_pairplot.png')
