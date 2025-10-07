import os
import sys
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point

# ログユーティリティ
sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
try:
    from log_message import log_message
except Exception:
    def log_message(msg: str, path: str):
        print(msg)


def extract_month_from_filename(path: str) -> str:
    basename = os.path.basename(path)
    return basename.split("_")[0]


def derive_place_year_from_gps_path(path: str) -> tuple[str, str]:
    # 既存スクリプトのロジックに合わせる: place は親ディレクトリ名から決定
    # file: .../<place_plus>/<year>/<month_...>.csv.gz
    place_plus = path.split('/')[-3]
    place = '_'.join(place_plus.split('_')[-3:-1])
    year = path.split('/')[-2]
    return place, year


def read_results_concat(result_files: list[str]) -> pd.DataFrame:
    frames = []
    for f in result_files:
        try:
            frames.append(pd.read_csv(f))
        except Exception as e:
            print(f"Failed to read result: {f} ({e})")
    if not frames:
        return pd.DataFrame(columns=[
            "hashed_adid",
            "segment_id",
            "point_ratio_in_rail_buffer",
            "point_ratio_in_bus_route_buffer",
            "point_ratio_in_bus_stop_buffer",
            "start_in_rail_buffer",
            "end_in_rail_buffer",
        ])
    return pd.concat(frames, ignore_index=True)


def read_gps_concat(gps_files: list[str]) -> pd.DataFrame:
    frames = []
    for f in gps_files:
        try:
            frames.append(pd.read_csv(f, parse_dates=["datetime"], compression="gzip"))
        except Exception as e:
            print(f"Failed to read gps: {f} ({e})")
    if not frames:
        return pd.DataFrame(columns=[
            "hashed_adid",
            "segment_id",
            "datetime",
            "longitude_anonymous",
            "latitude_anonymous",
        ])
    return pd.concat(frames, ignore_index=True)


def filter_segments(df_results: pd.DataFrame, mode: str) -> pd.DataFrame:
    if mode == "rail":
        mask = df_results["point_ratio_in_rail_buffer"] >= 1.0
    elif mode == "bus":
        mask = df_results["point_ratio_in_bus_route_buffer"] >= 1.0
    else:
        raise ValueError("mode must be 'rail' or 'bus'")
    cols = ["hashed_adid", "segment_id"]
    return df_results.loc[mask, cols].drop_duplicates()


def plot_segments(gps_df: pd.DataFrame, keep_pairs: pd.DataFrame, out_path: str, title: str, color: str) -> None:
    if gps_df.empty or keep_pairs.empty:
        print(f"No data to plot for {title}")
        return

    # フィルタ
    gps_keep = gps_df.merge(keep_pairs, on=["hashed_adid", "segment_id"], how="inner")
    if gps_keep.empty:
        print(f"No matching GPS points for {title}")
        return

    gdf = gpd.GeoDataFrame(
        gps_keep,
        geometry=gpd.points_from_xy(gps_keep["longitude_anonymous"], gps_keep["latitude_anonymous"]),
        crs="EPSG:4326",
    )

    fig, ax = plt.subplots(figsize=(10, 10))
    gdf.plot(ax=ax, color=color, markersize=2, alpha=0.7)
    ax.set_title(title)
    ax.set_axis_off()
    plt.tight_layout()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    plt.savefig(out_path, dpi=200)
    plt.close(fig)


def main():
    log_path = "/home/fukui/workspace/TravelModeEstimation/logs/log_09_gis.txt"

    files = sys.argv[1:]
    if "--" not in files:
        raise ValueError("Usage: python 02_transportation_filtering.py <gps_csv_gz ...> -- <trip_results_csv ...>")

    split_idx = files.index("--")
    gps_files = files[:split_idx]
    result_files = files[split_idx + 1:]

    if not gps_files or not result_files:
        raise ValueError("Both GPS files and result files are required.")

    # place/year は最初のGPSファイルから推定
    place, year = derive_place_year_from_gps_path(gps_files[0])
    out_dir = f"/home/data/fukui/outputs/figures/{place}/{year}/09_gis"
    os.makedirs(out_dir, exist_ok=True)

    # 月毎にファイルをグループ化
    month_to_gps = {}
    for f in gps_files:
        m = extract_month_from_filename(f)
        month_to_gps.setdefault(m, []).append(f)

    month_to_results = {}
    for f in result_files:
        m = extract_month_from_filename(f)
        month_to_results.setdefault(m, []).append(f)

    months = sorted(set(month_to_gps.keys()) & set(month_to_results.keys()))
    if not months:
        raise ValueError("No overlapping months between GPS and result files.")

    log_message(f"Months to process: {months}", log_path)

    # 集約用コンテナ
    aggregated_gps_frames = []
    aggregated_keep_rail = []
    aggregated_keep_bus = []

    for month in months:
        gps_paths = month_to_gps.get(month, [])
        res_paths = month_to_results.get(month, [])
        log_message(f"Processing month={month}: gps={len(gps_paths)} results={len(res_paths)}", log_path)

        # 読み込み
        df_results = read_results_concat(res_paths)
        df_gps = read_gps_concat(gps_paths)

        # 必要列チェック（最低限）
        required_gps_cols = {"hashed_adid", "segment_id", "longitude_anonymous", "latitude_anonymous"}
        if not required_gps_cols.issubset(df_gps.columns):
            missing = required_gps_cols - set(df_gps.columns)
            raise ValueError(f"GPS missing required columns: {missing}")

        required_res_cols = {"hashed_adid", "segment_id", "point_ratio_in_rail_buffer", "point_ratio_in_bus_route_buffer"}
        if not required_res_cols.issubset(df_results.columns):
            missing = required_res_cols - set(df_results.columns)
            raise ValueError(f"Results missing required columns: {missing}")

        # フィルタ（>=0.5）
        keep_rail = filter_segments(df_results, mode="rail")
        keep_bus = filter_segments(df_results, mode="bus")

        # 集約
        if not df_gps.empty:
            aggregated_gps_frames.append(df_gps)
        if not keep_rail.empty:
            aggregated_keep_rail.append(keep_rail)
        if not keep_bus.empty:
            aggregated_keep_bus.append(keep_bus)

    # すべての月を統合して最終プロット
    if aggregated_gps_frames:
        gps_all = pd.concat(aggregated_gps_frames, ignore_index=True)
    else:
        gps_all = pd.DataFrame(columns=[
            "hashed_adid", "segment_id", "datetime", "longitude_anonymous", "latitude_anonymous"
        ])

    keep_rail_all = pd.concat(aggregated_keep_rail, ignore_index=True).drop_duplicates() if aggregated_keep_rail else pd.DataFrame(columns=["hashed_adid", "segment_id"])
    keep_bus_all = pd.concat(aggregated_keep_bus, ignore_index=True).drop_duplicates() if aggregated_keep_bus else pd.DataFrame(columns=["hashed_adid", "segment_id"])

    rail_out_merged = os.path.join(out_dir, "merged_rail_segments.png")
    bus_out_merged = os.path.join(out_dir, "merged_bus_segments.png")

    plot_segments(
        gps_df=gps_all,
        keep_pairs=keep_bus_all,
        out_path=bus_out_merged,
        title=f"{place} {year} Bus-like segments merged (>=1.0)",
        color="blue",
    )


    plot_segments(
        gps_df=gps_all,
        keep_pairs=keep_rail_all,
        out_path=rail_out_merged,
        title=f"{place} {year} Rail-like segments merged (>=1.0)",
        color="green",
    )

    log_message("Finished 02_transportation_filtering", log_path)


if __name__ == "__main__":
    main()


