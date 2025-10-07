import time
import os
import sys
from math import radians, cos, sin, asin, sqrt
import pandas as pd

time_format = '%Y-%m-%d %H:%M:%S'

# structure of point
class Point:
    def __init__(self, latitude, longitude, datetime, hashed_adid):
        # 入力は度のまま保持（出力用）
        self.latitude = latitude
        self.longitude = longitude
        self.datetime = datetime
        self.hashed_adid = hashed_adid
        # 計算用にラジアンとUNIX秒を事前計算
        try:
            self.lat_rad = radians(float(latitude))
            self.lon_rad = radians(float(longitude))
        except Exception:
            self.lat_rad = radians(float(latitude))
            self.lon_rad = radians(float(longitude))
        try:
            # pandas.Timestamp なら高速に処理される
            self.ts = pd.to_datetime(datetime).timestamp()
        except Exception:
            self.ts = pd.to_datetime(datetime).timestamp()
        
# calculate distance between two points from their coordinate
def getDistanceOfPoints(pi, pj):
    lat1, lon1, lat2, lon2 = pi.lat_rad, pi.lon_rad, pj.lat_rad, pj.lon_rad
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    m = 6371000 * c
    return m

# calculate time interval between two points
def getTimeIntervalOfPoints(pi, pj):
    return pj.ts - pi.ts

# i..j のクラスタが distThres 以内かを判定（越えたら即終了）
def is_within_threshold(points, i, j, distThres):
    # 早期終了を入れた二重ループ
    for n in range(i + 1, j + 1):
        pn = points[n]
        for m in range(i, n):
            if getDistanceOfPoints(points[m], pn) > distThres:
                return False
    return True

def stayPointExtraction(points, distThres, timeThres):
    stayPointList = []
    N = len(points)
    i = 0

    while i < N:
        # j*: 最小のj (時間差 >= timeThres)
        j = i + 1
        while j < N and getTimeIntervalOfPoints(points[i], points[j]) < timeThres:
            j += 1
        if j >= N:
            break

        # 判定
        if not is_within_threshold(points, i, j, distThres):
            i += 1
        else:
            # j*: 最大のj (Diameter <= distThres)
            while j + 1 < N:
                new_idx = j + 1
                pn = points[new_idx]
                ok = True
                for m in range(i, new_idx):
                    if getDistanceOfPoints(points[m], pn) > distThres:
                        ok = False
                        break
                if ok:
                    j = new_idx
                else:
                    break
            # 滞在に含まれるすべての点を追加
            stayPointList.extend(points[i:j+1])
            i = j + 1

    return pd.DataFrame([{
        'hashed_adid': p.hashed_adid,
        'latitude': p.latitude,
        'longitude': p.longitude,
        'datetime': p.datetime
    } for p in stayPointList])


#density
    # while i < pointNum - 1: 
    #     # j: index of the last point within distTres
    #     j = i + 1
    #     flag = False
    #     while j < pointNum:
    #         if getDistanceOfPoints(points[i], points[j]) < distThres:
    #             j += 1
    #         else:
    #             break

    #     j -= 1
    #     # at least one point found within distThres
    #     if j > i:
    #         # candidate cluster found
    #         if getTimeIntervalOfPoints(points[i], points[j]) > timeThres: 
    #             nexti = i + 1
    #             j += 1
    #             while j < pointNum:
    #                 if getDistanceOfPoints(points[nexti], points[j]) < distThres and \
    #                     getTimeIntervalOfPoints(points[nexti], points[j]) > timeThres: 
    #                     nexti += 1
    #                     j += 1
    #                 else:
    #                     break
    #             j -= 1
    #             stayPointList.extend(points[i : j+1])
    #     i = j + 1