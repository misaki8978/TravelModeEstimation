import pandas as pd
import numpy as np
from math import sin, cos, atan2, sqrt

# 距離計算
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000.0
    # lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

#ポイント間の距離を計算
def calculate_total_distance(segment):
    total_dist = 0.0
    for i in range(1, len(segment)):
        lat1 = segment.iloc[i - 1]['latitude_anonymous']
        lon1 = segment.iloc[i - 1]['longitude_anonymous']
        lat2 = segment.iloc[i]['latitude_anonymous']
        lon2 = segment.iloc[i]['longitude_anonymous']
        total_dist += haversine_distance(lat1, lon1, lat2, lon2)
    return total_dist

def segment_by_label(df, v_thd, a_thd):
    df = df.sort_values('datetime')\
            .assign(
                    acceleration=lambda x: x['P_speed'].diff(1)/x['time_diff_s'],
                    label=lambda x: np.where((x['P_speed'] < v_thd) & (x['acceleration'] < a_thd),\
                     'walk', 'non-walk')
                    )
    
    segments = []
    current_label = df.iloc[0]['label']
    current_segment = [df.iloc[0]]

    for i in range(1, len(df)):
        row = df.iloc[i]
        if row['label'] == current_label:
            current_segment.append(row)
        else:
            segments.append(pd.DataFrame(current_segment))
            current_label = row['label']
            current_segment = [row]

    if current_segment:
        segments.append(pd.DataFrame(current_segment))

    return segments


# 4. 距離がしきい値以下のセグメントに "short" ラベルをつける + 連続 short をマージ
def merge_short_segments(segments, dist_thd1):
    merged = []
    i = 0
    while i < len(segments):
        current = segments[i].assign(
            length=lambda x: calculate_total_distance(x),
            label=lambda x: np.where(x['length'] < dist_thd1, 'short', x["label"])
        )   # 距離がしきい値以下のセグメントに "short" ラベルをつける

        # 連続 short のマージ
        while (i + 1 < len(segments) and
               current['label'].iloc[0] == 'short' and
               segments[i + 1]['label'].iloc[0] == 'short'):
            next_seg = segments[i + 1]
            current = pd.concat([current, next_seg])\
                        .assign(
                                length=lambda x: calculate_total_distance(x),
                                label='short'
                                )
            i += 1

        merged.append(current)
        i += 1
    return merged

# 5. 非連続な short セグメントを次のセグメントにマージ
def merge_short_to_next_segment(segments, dist_thd1):
    result = []
    i = 0
    while i < len(segments):
        current = segments[i]
        current_label = current['label'].iloc[0]
        current_length = current['length'].iloc[0] if 'length' in current else calculate_total_distance(current)

        if (current_label == 'short' and current_length < dist_thd1 and
            (i + 1 >= len(segments) or segments[i + 1]['label'].iloc[0] != 'short')):

            if i + 1 < len(segments):
                next_seg = segments[i + 1]
                merged = pd.concat([current, next_seg])\
                            .assign(
                                label=next_seg['label'].iloc[0],
                                length=lambda x: calculate_total_distance(x)
                            )\
                            .sort_values('datetime')
                result.append(merged)
                i += 2
            else:
                result.append(current)
                i += 1
        else:
            result.append(current)
            i += 1
    return result



def merge_uncertain_segments(segments, dist_thd2, limit_thd):
    merged_result = []
    i = 0

    for seg in segments:
        seg['label_cer'] = 'uncertain' if seg['length'].iloc[0] < dist_thd2 else 'certain'

    # ---- 1. まず uncertain → walk に変更（フラグは保持） ---------------
    for seg in segments:
        if seg['label_cer'].iloc[0] == 'uncertain' and seg['label_cer'].iloc[0] == 'uncertain':
            seg['label'] = 'walk'

    # ---- 2. run を数えて条件に応じてマージ／再ラベル --------------------
    while i < len(segments):
        seg = segments[i]

        if seg['label_cer'].iloc[0] == 'uncertain':
            # run の開始
            start = i
            while i < len(segments) and segments[i]['label_cer'].iloc[0] == 'uncertain':
                i += 1
            run_len = i - start  # run 内セグメント数

            if run_len > limit_thd:
                # run 全体をマージし non-walk に
                run_segments = segments[start:i]
                merged = (
                    pd.concat(run_segments, ignore_index=True)
                      .sort_values('datetime')
                      .reset_index(drop=True)
                )
                merged['length'] = calculate_total_distance(merged)
                merged['label']  = 'non-walk'
                # label_cer は 'uncertain' のまま保持
                merged_result.append(merged)
            else:
                # run 長が閾値以下 → 個別 walk のまま
                merged_result.extend(segments[start:i])
        else:
            merged_result.append(seg)
            i += 1

    return merged_result

def merge_consecutive_same_label_segments(segments):
    if not segments:
        return []

    merged_segments = []
    current_segment = segments[0].copy()

    for next_segment in segments[1:]:
        if current_segment['label'].iloc[0] == next_segment['label'].iloc[0]:
            current_segment = pd.concat([current_segment, next_segment], ignore_index=True)
        else:
            merged_segments.append(current_segment)
            current_segment = next_segment.copy()

    merged_segments.append(current_segment)
    return merged_segments



def assign_segment_ids_by_move(segments, segment_id):

    for seg in segments:
        seg["segment_id"] = segment_id
        segment_id += 1
    
    next_segment_id = segment_id

    return segments, next_segment_id


# 8. move_id ごとにすべての処理を実行する関数
def process_all_segments(df, v_thd, a_thd, dist_thd1, dist_thd2, limit_thd):
    all_results = []
    segment_id = 0
    for move_id, group in df.groupby('move_id'):
        move_points = segment_by_label(group, v_thd, a_thd)
        segments = merge_short_segments(move_points, dist_thd1)
        segments = merge_short_to_next_segment(segments, dist_thd1)
        segments = merge_uncertain_segments(segments, dist_thd2, limit_thd) # ここでは limit_thd は使わない
        segments = merge_consecutive_same_label_segments(segments)
        segments, segment_id = assign_segment_ids_by_move(segments, segment_id)

        all_results.extend(segments)

    results = pd.concat(all_results)\
                .sort_values(['move_id', 'datetime'])\
                .reset_index(drop=True)
    final_df = results[["hashed_adid", "datetime", "move_id", "segment_id", "label", "label_cer",  "P_speed", "acceleration", "latitude_anonymous", "longitude_anonymous", "accuracy"]]
    return final_df