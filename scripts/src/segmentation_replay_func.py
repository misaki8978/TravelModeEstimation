from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import numpy.typing as npt
import pandas as pd


# =========================
#  Segment dataclass
# =========================
@dataclass
class Segment:
    start: int
    end: int
    label: int      # 1 = walk, 0 = non-walk
    length_m: float


# =========================
#  Haversine Distance
# =========================
def haversine_distance(
    lat1: npt.NDArray[np.float64],
    lon1: npt.NDArray[np.float64],
    lat2: npt.NDArray[np.float64],
    lon2: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    """Compute great-circle distance in meters."""
    R = 6371000.0  # Earth radius in meters

    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)

    a = (
        np.sin(dphi / 2.0) ** 2
        + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    )
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))

    return (R * c).astype(np.float64)


# =========================
#  初期 walk / non-walk セグメント
# =========================
def _build_initial_segments(
    walk_flags: npt.NDArray[np.int_],
    dist_m: npt.NDArray[np.float64],
) -> List[Segment]:

    n = len(walk_flags)
    if n == 0:
        return []

    segments: List[Segment] = []
    current_label = int(walk_flags[0])
    seg_start = 0

    for i in range(1, n):
        label_i = int(walk_flags[i])
        if label_i != current_label:
            seg_end = i - 1
            length = float(dist_m[seg_start: seg_end + 1].sum())
            segments.append(Segment(seg_start, seg_end, current_label, length))
            seg_start = i
            current_label = label_i

    # last segment
    seg_end = n - 1
    length = float(dist_m[seg_start: seg_end + 1].sum())
    segments.append(Segment(seg_start, seg_end, current_label, length))

    return segments


# =========================
#  2 セグメント統合
# =========================
def _merge_two_segments(
    segments: List[Segment],
    i: int,
    j: int,
    dist_m: npt.NDArray[np.float64],
) -> List[Segment]:

    if i > j:
        i, j = j, i

    seg_i = segments[i]
    seg_j = segments[j]

    start = seg_i.start
    end = seg_j.end

    label = seg_i.label if seg_i.length_m >= seg_j.length_m else seg_j.label
    length = float(dist_m[start: end + 1].sum())

    merged = Segment(start, end, label, length)

    return segments[:i] + [merged] + segments[j + 1:]


# =========================
#  very short segment removal
# =========================
def _remove_very_short_segments(
    segments: List[Segment],
    dist_m: npt.NDArray[np.float64],
    dist_thd1: float,
) -> List[Segment]:
    """短すぎるセグメントを隣接セグメントにマージして除去する."""

    # セグメント数 0 or 1 なら何もできない
    if len(segments) <= 1:
        return segments

    changed = True
    while changed:
        changed = False

        # ここから下のインデントに注意！！すべて while の中
        for idx, seg in enumerate(segments):
            # 閾値以上の長さならスキップ
            if seg.length_m >= dist_thd1:
                continue

            # ここに来た時点で「短すぎるセグメント」を見つけた

            # segments が 1 個になっていたらマージできない
            if len(segments) == 1:
                return segments

            if idx == 0:
                # 先頭 → 次のセグメントとマージ
                if len(segments) > 1:
                    segments = _merge_two_segments(segments, 0, 1, dist_m)
                else:
                    return segments
            elif idx == len(segments) - 1:
                # 末尾 → 一つ前のセグメントとマージ
                if len(segments) > 1:
                    segments = _merge_two_segments(segments, idx - 1, idx, dist_m)
                else:
                    return segments
            else:
                # 中間 → 前後どちらか長いほうとマージ
                prev_seg = segments[idx - 1]
                next_seg = segments[idx + 1]
                if prev_seg.length_m >= next_seg.length_m:
                    segments = _merge_two_segments(segments, idx - 1, idx, dist_m)
                else:
                    segments = _merge_two_segments(segments, idx, idx + 1, dist_m)

            changed = True
            # リストが書き換わったので、for ループを抜けて while の先頭からやり直し
            break

    return segments


# =========================
#  uncertain run merging
# =========================
def _merge_uncertain_runs(
    segments: List[Segment],
    dist_m: npt.NDArray[np.float64],
    dist_thd2: float,
    limit_thd: int,
) -> List[Segment]:

    if not segments:
        return []

    is_uncertain = [seg.length_m < dist_thd2 for seg in segments]
    new_segs: List[Segment] = []

    i = 0
    n = len(segments)

    while i < n:
        if not is_uncertain[i]:
            new_segs.append(segments[i])
            i += 1
            continue

        j = i
        while j < n and is_uncertain[j]:
            j += 1

        run_len = j - i

        if run_len >= limit_thd:
            start = segments[i].start
            end = segments[j - 1].end
            length = float(dist_m[start: end + 1].sum())
            new_segs.append(Segment(start, end, 0, length))  # 0 = non-walk
        else:
            new_segs.extend(segments[i:j])

        i = j

    return new_segs


# =========================
#  1 move の segmentation
# =========================
def _segment_single_move(
    lat: npt.NDArray[np.float64],
    lon: npt.NDArray[np.float64],
    t: npt.NDArray[np.float64],
    start_segment_id: int,
    v_thd: float,
    a_thd: float,
    dist_thd1: float,
    dist_thd2: float,
    limit_thd: int,
) -> Tuple[npt.NDArray[np.int_], npt.NDArray[np.int_]]:
    """1つの move_id 内のポイント列をセグメンテーションし、
    segment_id と walk/non-walk ラベルを返す。
    """

    n = len(lat)
    if n == 0:
        return np.array([], dtype=int), np.array([], dtype=int)
    if n == 1:
        return (
            np.array([start_segment_id], dtype=int),
            np.array([1], dtype=int),  # 単一点はとりあえず walk とする
        )

    lat = lat.astype(float)
    lon = lon.astype(float)
    t = t.astype(float)

    # distance from previous point
    dist_m = np.zeros(n)
    dist_m[1:] = haversine_distance(lat[:-1], lon[:-1], lat[1:], lon[1:])

    # time difference
    dt = np.zeros(n)
    dt[1:] = np.diff(t)
    dt[dt <= 0] = np.nan

    # velocity
    v = np.zeros(n)
    v[1:] = dist_m[1:] / dt[1:]
    v[~np.isfinite(v)] = 0.0

    # acceleration
    dv = np.zeros(n)
    dv[1:] = v[1:] - v[:-1]
    a = np.zeros(n)
    a[1:] = dv[1:] / dt[1:]
    a[~np.isfinite(a)] = 0.0

    # 初期の walk / non-walk ラベル
    walk_flags = ((v < v_thd) & (a < a_thd)).astype(int)

    # セグメント構築 & マージ
    segments = _build_initial_segments(walk_flags, dist_m)
    segments = _remove_very_short_segments(segments, dist_m, dist_thd1)
    segments = _merge_uncertain_runs(segments, dist_m, dist_thd2, limit_thd)

    # 各点への segment_id & walk ラベル付与（最終版）
    seg_ids = np.empty(n, dtype=int)
    walk_labels = np.empty(n, dtype=int)  # 1=walk, 0=non-walk

    current_id = start_segment_id
    for seg in segments:
        seg_ids[seg.start: seg.end + 1] = current_id
        walk_labels[seg.start: seg.end + 1] = seg.label
        current_id += 1

    return seg_ids, walk_labels


# =========================
#  メイン関数
# =========================
def apply_moisp_segmentation(
    df: pd.DataFrame,
    v_thd: float,
    a_thd: float,
    dist_thd1: float,
    dist_thd2: float,
    limit_thd: int,
    move_col: str = "move_id",
    lat_col: str = "latitude_anonymous",
    lon_col: str = "longitude_anonymous",
    time_col: str = "datetime",
    start_segment_id: int = 0,
) -> pd.DataFrame:
    """MoISP セグメンテーションを各 move_id ごとに適用し、
    segment_id と is_walk を付与した DataFrame を返す。

    Args:
        df: 入力 DataFrame（1ユーザ分）
        v_thd: 速度閾値 [m/s]
        a_thd: 加速度閾値 [m/s^2]
        dist_thd1: 「非常に短いセグメント」とみなす距離閾値 [m]
        dist_thd2: 不確かなセグメントの距離閾値 [m]
        limit_thd: 不確かなセグメントが何個続いたらまとめて non-walk にするか
        move_col, lat_col, lon_col, time_col: 列名
        start_segment_id: segment_id の開始値（グローバル）

    Returns:
        segment_id（int）、is_walk（0/1）が追加された DataFrame のコピー
    """

    out = df.copy()
    if out.empty:
        out["segment_id"] = np.array([], dtype=int)
        out["is_walk"] = np.array([], dtype=int)
        return out

    # ソート
    out = out.sort_values([move_col, time_col]).reset_index(drop=True)

    # datetime → POSIX 秒
    t_raw = out[time_col]
    if not np.issubdtype(t_raw.dtype, np.datetime64):
        t_raw = pd.to_datetime(t_raw)
    t_sec = t_raw.view("int64") / 1e9
    t_sec = t_sec.to_numpy(dtype=float)

    segment_ids_all = np.empty(len(out), dtype=int)
    is_walk_all = np.empty(len(out), dtype=int)

    current_seg_id = start_segment_id

    # move_id ごとに処理
    for _, idx_group in out.groupby(move_col).groups.items():
        idx = np.array(sorted(idx_group))

        lat = out.loc[idx, lat_col].to_numpy(dtype=float)
        lon = out.loc[idx, lon_col].to_numpy(dtype=float)
        t = t_sec[idx]

        seg_ids, walk_labels = _segment_single_move(
            lat=lat,
            lon=lon,
            t=t,
            start_segment_id=current_seg_id,
            v_thd=v_thd,
            a_thd=a_thd,
            dist_thd1=dist_thd1,
            dist_thd2=dist_thd2,
            limit_thd=limit_thd,
        )

        segment_ids_all[idx] = seg_ids
        is_walk_all[idx] = walk_labels
        current_seg_id = int(seg_ids.max()) + 1

    out["segment_id"] = segment_ids_all
    out["is_walk"] = is_walk_all  # 1=walk, 0=non-walk

    return out
