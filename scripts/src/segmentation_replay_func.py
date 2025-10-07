import pandas as pd
import numpy as np

def compute_speed_acc(df):
    df = df.sort_values("datetime").reset_index(drop=True)
    df["time_diff_s"] = df["datetime"].diff().dt.total_seconds()
    df["speed"] = df["distance_m"]/df["time_diff_s"]
    df["acceleration"] = df["speed"].diff()/df["time_diff_s"]
    return df

# ---- Step 1 ----
def segment_by_label(df, v_thd, a_thd):
    df = compute_speed_acc(df)
    df["label"] = np.where((df["speed"] <= v_thd) & (df["acceleration"] <= a_thd), "walk", "non-walk")
    return df

# ---- Step 2 ----
def merge_short_segments(df, dist_thd1):
    df["segment_tmp"] = (df["label"] != df["label"].shift()).cumsum()
    results = []
    for _, seg in df.groupby("segment_tmp"):
        if seg["distance_m"].sum() < dist_thd1:
            prev_label = results[-1]["label"].iloc[0] if results else "non-walk"
            seg["label"] = prev_label
        results.append(seg)
    merged = pd.concat(results)
    merged = merged.drop(columns=["segment_tmp"])
    return merged

def merge_short_to_next_segment(df, dist_thd1):
    df["segment_tmp"] = (df["label"] != df["label"].shift()).cumsum()
    results = []
    groups = list(df.groupby("segment_tmp"))
    skip_next = False
    for i, (sid, seg) in enumerate(groups):
        if skip_next:
            skip_next = False
            continue
        if seg["distance_m"].sum() < dist_thd1 and i + 1 < len(groups):
            next_seg = groups[i + 1][1]
            seg["label"] = next_seg["label"].iloc[0]
            combined = pd.concat([seg, next_seg])
            results.append(combined)
            skip_next = True
        else:
            results.append(seg)
    merged = pd.concat(results)
    merged = merged.drop(columns=["segment_tmp"])
    return merged

# ---- Step 3 ----
def merge_uncertain_segments(df, dist_thd2, limit_thd):
    df["segment_tmp"] = (df["label"] != df["label"].shift()).cumsum()
    results = []
    uncertain_buffer = []
    for _, seg in df.groupby("segment_tmp"):
        total_dist = seg["distance_m"].sum()
        if total_dist < dist_thd2:
            uncertain_buffer.append(seg)
        else:
            if len(uncertain_buffer) >= limit_thd:
                merged_uncertain = pd.concat(uncertain_buffer)
                merged_uncertain["label"] = "non-walk"
                results.append(merged_uncertain)
            elif len(uncertain_buffer) > 0:
                results.extend(uncertain_buffer)
            uncertain_buffer = []
            results.append(seg)
    if len(uncertain_buffer) >= limit_thd:
        merged_uncertain = pd.concat(uncertain_buffer)
        merged_uncertain["label"] = "non-walk"
        results.append(merged_uncertain)
    elif len(uncertain_buffer) > 0:
        results.extend(uncertain_buffer)
    merged = pd.concat(results)
    merged = merged.drop(columns=["segment_tmp"])
    return merged

def merge_consecutive_same_label_segments(df):
    df["segment_tmp"] = (df["label"] != df["label"].shift()).cumsum()
    merged = df.drop(columns=["segment_tmp"])
    return merged

# ---- Step 4 ----
def assign_segment_ids_by_move(df, start_id):
    df["segment_tmp"] = (df["label"] != df["label"].shift()).cumsum()
    segment_id_map = {sid: i for i, sid in enumerate(df["segment_tmp"].unique(), start=start_id)}
    df["segment_id"] = df["segment_tmp"].map(segment_id_map)
    df = df.drop(columns=["segment_tmp"])
    return df, max(segment_id_map.values()) + 1

# ---- Main ----
def process_all_segments(df, v_thd, a_thd, dist_thd1, dist_thd2, limit_thd):
    all_results = []
    segment_id = 0
    for move_id, group in df.groupby('move_id'):
        move_points = segment_by_label(group, v_thd, a_thd)
        segments = merge_short_segments(move_points, dist_thd1)
        segments = merge_short_to_next_segment(segments, dist_thd1)
        segments = merge_uncertain_segments(segments, dist_thd2, limit_thd)
        segments = merge_consecutive_same_label_segments(segments)
        segments, segment_id = assign_segment_ids_by_move(segments, segment_id)
      
      
      
        all_results.append(segments)

    results = pd.concat(all_results).sort_values(["move_id", "datetime"]).reset_index(drop=True)
    final_df = results[[
        "hashed_adid", "datetime", "move_id", "segment_id", "label",
        "speed", "acceleration", "distance_m", "time_diff_s",
        "latitude_anonymous", "longitude_anonymous", "accuracy"
    ]]
    return final_df


# -------------------- hashed_adidごとの実行例 --------------------
# all_outputs = []
# for uid, g in df.groupby("hashed_adid"):
#     processed = process_all_segments(g, v_thd, a_thd, dist_thd1, dist_thd2, limit_thd)
#     all_outputs.append(processed)
# final_result = pd.concat(all_outputs).reset_index(drop=True)
