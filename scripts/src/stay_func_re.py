import time
import os
import sys
from math import radians, cos, sin, asin, sqrt
import pandas as pd

time_format = '%Y-%m-%d %H:%M:%S'

# structure of point
class Point:
    def __init__(self, latitude, longitude, datetime, hashed_adid):
        self.latitude = latitude
        self.longitude = longitude
        self.datetime = datetime
        self.hashed_adid = hashed_adid
        
# calculate distance between two points from their coordinate
def getDistanceOfPoints(pi, pj):
    lat1, lon1, lat2, lon2 = list(map(radians, [float(pi.latitude), float(pi.longitude),
                                                float(pj.latitude), float(pj.longitude)]))
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    m = 6371000 * c
    return m

# calculate time interval between two points
def getTimeIntervalOfPoints(pi, pj):
    t_i = pd.to_datetime(pi.datetime).timestamp()
    t_j = pd.to_datetime(pj.datetime).timestamp()
    return t_j - t_i

# extract stay points from a GPS log file
# input:
#        file: the name of a GPS log file
#        distThres: distance threshold
#        timeThres: time span threshold
# default values of distThres and timeThres are 200 m and 30 min respectively, according to [1]
def stayPointExtraction(points, distThres, timeThres):
    stayPointList = []
    pointNum = len(points)
    i = 0
    while i < pointNum:
        j = i + 1
        while j < pointNum:
            if getDistanceOfPoints(points[i], points[j]) > distThres:
                # points[j] has gone out of bound thus it should not be counted in the stay points.
                if getTimeIntervalOfPoints(points[i], points[j-1]) > timeThres:
                    stayPointList.extend(points[i:j])
                break
            j += 1
        i = j
    return pd.DataFrame([{
        'hashed_adid': p.hashed_adid,
        'latitude': p.latitude,
        'longitude': p.longitude,
        'datetime': p.datetime
    } for p in stayPointList])
