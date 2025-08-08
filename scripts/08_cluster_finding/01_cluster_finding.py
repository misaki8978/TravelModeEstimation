import os
import sys
import pandas as pd
import gzip
from datetime import datetime
import plotly.graph_objs as go
import warnings
import matplotlib.colors as mcolors  # ← 追加

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

warnings.filterwarnings('ignore')

log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/log_08_cluster_finding.txt"

# --- 引数解析 ---
files = sys.argv[1:]

path_parts = files[0].split("/")
log_message(f"path_parts: {path_parts}", log_path)
year = path_parts[-2]
place_ = path_parts[-3]
place = "_".join(place_.split("_")[2:])

OUT_DIR = f"/home/data/fukui/processed/08_01_{place}/{year}"
os.makedirs(OUT_DIR, exist_ok=True)

# --- データ読み込み ---
df_list = []
for file in files:
    with gzip.open(file, 'rt') as f:
        df = pd.read_csv(f, dtype={
            "latitude": float,
            "longitude": float
        }).query("max_membership >= 0.9")
        df_list.append(df)

df = pd.concat(df_list)
df = df.sort_values(["hashed_adid", "segment_id", "datetime"])

# --- カラーパレット（RGBで定義） ---
base_colors = [
    mcolors.to_rgb('#417038'),  # 緑
    mcolors.to_rgb('#ba8b40'),  # 黄
    mcolors.to_rgb('#942343'),  # 赤
    mcolors.to_rgb('#043c78')   # 紺
]

# --- クラスタごとに個別のマップを生成・保存 ---
clusters = df["fuzzy_cluster"].unique()

for cluster in clusters:
    cluster_data = df[df["fuzzy_cluster"] == cluster]
    base_color = base_colors[int(cluster) % len(base_colors)]

    # --- max_membershipをクラスタ内で標準化（0〜1） ---
    memberships = cluster_data["max_membership"]
    if memberships.max() != memberships.min():
        scaled_membership = (memberships - memberships.min()) / (memberships.max() - memberships.min())
    else:
        scaled_membership = pd.Series(1.0, index=memberships.index)

    # --- RGBAカラー生成（透明度 = スケール済 max_membership） ---
    rgba_colors = [
        f"rgba({int(base_color[0]*255)}, {int(base_color[1]*255)}, {int(base_color[2]*255)}, {opacity})"
        for opacity in scaled_membership
    ]

    trace = go.Scattermapbox(
        lat=cluster_data["latitude"],
        lon=cluster_data["longitude"],
        mode="markers",
        marker=dict(
            size=6,
            color=rgba_colors
        ),
        text=[
            f"adid: {adid}<br>segment: {segment_id}<br>cluster: {cluster}<br>membership(raw): {m:.2f}<br>membership(scaled): {s:.2f}"
            for adid, segment_id, m, s in zip(
                cluster_data["hashed_adid"],
                cluster_data["segment_id"],
                memberships,
                scaled_membership
            )
        ],
        hoverinfo="text",
        name=f"Cluster {cluster+1}",
        showlegend=False
    )

    layout = go.Layout(
        mapbox=dict(
            style="open-street-map",
            center=dict(lat=cluster_data["latitude"].mean(), lon=cluster_data["longitude"].mean()),
            zoom=10
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        title=f"Fuzzy Cluster {cluster+1}",
    )

    fig = go.Figure(data=[trace], layout=layout)
    fig.write_html(f"{OUT_DIR}/cluster{cluster+1}_finding.html")
