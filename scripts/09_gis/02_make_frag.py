import os
import sys
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

import osmnx as ox

import warnings
warnings.filterwarnings('ignore')

sys.path.append("/home/fukui/workspace/TravelModeEstimation/scripts/src")
from log_message import log_message

log_path  = f"/home/fukui/workspace/TravelModeEstimation/logs/log_09_make_frag.txt"

def extract_month(path: str) -> str:
    basename = os.path.basename(path)
    return basename.split('_')[0]


def derive_place_year_from_gps(path: str) -> tuple[str, str]:
    place_plus = path.split('/')[-3]
    place = '_'.join(place_plus.split('_')[-3:-1])
    year = path.split('/')[-2]
    return place, year


def read_gps_concat(paths: list[str]) -> pd.DataFrame:
    frames = []
    for p in paths:
        try:
            df = pd.read_csv(p, compression='gzip', dtype={"hashed_adid": str, "segment_id": str})
            frames.append(df)
        except Exception as e:
            log_message(f"Failed to read GPS: {p} ({e})", log_path)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def read_results_concat(paths: list[str]) -> pd.DataFrame:
    frames = []
    for p in paths:
        try:
            df = pd.read_csv(p, dtype={"hashed_adid": str, "segment_id": str})
            frames.append(df)
        except Exception as e:
            log_message(f"Failed to read results: {p} ({e})", log_path)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def main():
    files = sys.argv[1:]
    if '--' not in files:
        raise ValueError("Usage: python 02_make_frag.py <gps_csv_gz ...> -- <gis_result_csv ...>")

    split_idx = files.index('--')
    gps_files, gis_files = files[:split_idx], files[split_idx + 1:]
    if not gps_files:
        raise ValueError("GPS files are required")
    log_message(f"gps_files: {len(gps_files)}, gis_files: {len(gis_files)}", log_path)

    # place/year は最初のGPSから決定
    place, year = derive_place_year_from_gps(gps_files[0])
    out_dir = f"/home/data/fukui/processed/09_02_{place}/{year}"
    os.makedirs(out_dir, exist_ok=True)

    # 月ごとにグルーピング
    month_to_gps: dict[str, list[str]] = {}
    for p in gps_files:
        m = extract_month(p)
        month_to_gps.setdefault(m, []).append(p)

    month_to_gis: dict[str, list[str]] = {}
    for p in gis_files:
        m = extract_month(p)
        month_to_gis.setdefault(m, []).append(p)

    months = sorted(set(month_to_gps.keys()) | set(month_to_gis.keys()))
    log_message(f"Months to process: {months}", log_path)

    for month in months:
        gps_paths = month_to_gps.get(month, [])
        gis_paths = month_to_gis.get(month, [])
        log_message(f"Processing month={month}: gps={len(gps_paths)} gis={len(gis_paths)}", log_path)

        df_gps = read_gps_concat(gps_paths)
        if df_gps.empty:
            log_message(f"Skip month={month} because GPS is empty", log_path)
            continue

        # hashed_adid, segment_id の存在確認
        required_gps_cols = {"hashed_adid", "segment_id"}
        missing_gps = required_gps_cols - set(df_gps.columns)
        if missing_gps:
            raise ValueError(f"GPS missing required columns: {missing_gps}")

        # 既定値（0）で初期化
        df_gps["rail_frag"] = 0
        df_gps["bus_frag"] = 0

        if gis_paths:
            df_res = read_results_concat(gis_paths)
            if not df_res.empty:
                # 必要列が揃っているか確認
                required_res_cols = {"hashed_adid", "segment_id", "point_ratio_in_rail_buffer", "point_ratio_in_bus_route_buffer"}
                missing_res = required_res_cols - set(df_res.columns)
                if missing_res:
                    log_message(f"Results missing columns {missing_res} for month={month}. Flags will stay 0.", log_path)
                else:
                    # 閾値で抽出してユニーク化
                    rail_pairs = (
                        df_res.loc[df_res["point_ratio_in_rail_buffer"] >= 0.8, ["hashed_adid", "segment_id"]]
                        .drop_duplicates()
                        .assign(rail_frag=1)
                    )
                    bus_pairs = (
                        df_res.loc[df_res["point_ratio_in_bus_route_buffer"] >= 0.8, ["hashed_adid", "segment_id"]]
                        .drop_duplicates()
                        .assign(bus_frag=1)
                    )

                    # マージ（m:1 を期待）
                    if not rail_pairs.empty:
                        df_gps = df_gps.merge(rail_pairs, on=["hashed_adid", "segment_id"], how="left")
                        df_gps["rail_frag"] = df_gps["rail_frag_y"].fillna(df_gps["rail_frag_x"]).fillna(0).astype(int)
                        df_gps = df_gps.drop(columns=[c for c in ["rail_frag_x", "rail_frag_y"] if c in df_gps.columns])
                    if not bus_pairs.empty:
                        df_gps = df_gps.merge(bus_pairs, on=["hashed_adid", "segment_id"], how="left")
                        df_gps["bus_frag"] = df_gps["bus_frag_y"].fillna(df_gps["bus_frag_x"]).fillna(0).astype(int)
                        df_gps = df_gps.drop(columns=[c for c in ["bus_frag_x", "bus_frag_y"] if c in df_gps.columns])

            else:
                log_message(f"Result empty for month={month}. Flags remain 0.", log_path)
        else:
            log_message(f"No GIS results provided for month={month}. Flags remain 0.", log_path)

        # 出力（gzip）
        out_path = os.path.join(out_dir, f"{month}_gps_with_frags.csv.gz")
        df_gps.to_csv(out_path, index=False, compression='gzip')
        log_message(f"Wrote: {out_path} rows={len(df_gps)}", log_path)


if __name__ == "__main__":
    main()

