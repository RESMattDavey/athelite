# -*- coding: utf-8 -*-
"""
Created on Sat Dec 24 15:11:41 2016

@author: Matthew
"""


import sys
import pandas as pd
import math
import datetime as dt
import numpy as np
from matplotlib import pyplot as plt
sys.path.append('C:\repositories\pygpx')
import pygpx
GPX = pygpx.GPX
from sqlalchemy import create_engine
from sqlalchemy import TIMESTAMP
import os

def calc_dist(row):
    lat1 = row['lat']
    lon1 = row['lon']
    lat2 = row['lat2']
    lon2 = row['lon2']
    radius = 6378700.0 # meters
    if ((lat1==lat2) & (lon1==lon2)):
        d = 0
    else:
        dlat = math.radians(lat2-lat1)
        dlon = math.radians(lon2-lon1)
        a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
            * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = radius * c * 1.
    return d
    
def calc_trimp(gpx_with_hr, gender, hr_max, hr_rest): 
    if gender == 'm':
        k = 1.92
    else: k = 1.67
    hr_cap = hr_max-hr_rest
    tracks = gpx.tracks
    cols = ['lat','lon','z','hr']
    track_df = pd.DataFrame(columns = cols)
    for track in tracks:
        for trkseg in track.trksegs:
            for trkpnt in trkseg.trkpts:
                track_df = track_df.append(pd.DataFrame([[trkpnt.lat,trkpnt.lon,trkpnt.elevation,trkpnt.hr]],index = [trkpnt.time],columns = cols))
                
    #%%
    track_df['tvalue'] = track_df.index
    track_df['dt'] = (track_df['tvalue']-track_df['tvalue'].shift()).fillna(0)
    track_df['dtS'] = track_df.dt.dt.total_seconds() 
    track_df['dtM'] = track_df.dt.dt.total_seconds() / 60.
    track_df['HRR'] = (track_df.hr-hr_rest)/(hr_cap)
    track_df.HRR = track_df.HRR * track_df.dtM
    track_df['hr10'] = track_df.hr.round(-1)
    track_df['imp'] = track_df.HRR.apply(lambda x: x*0.64*math.exp(k*x)) 
    trimp = track_df.imp.sum()    
    track_df['lat2'] = track_df.lat.shift(1).fillna(0)
    track_df['lon2'] = track_df.lon.shift(1).fillna(0)
    track_df['metres'] = track_df.apply(lambda row: calc_dist(row), axis = 1)
    track_df.metres = track_df.metres.fillna(0)
    track_df['speed'] = track_df.metres/track_df.dtS
    track_df['sp1']= track_df.speed.divide(5).round(1).multiply(5)
 
    #fig, ax1 = plt.subplots()

    #ax1.hist(track_df.sp1.replace([np.inf, -np.inf], np.nan).dropna())
    #plt.show()
    #fig2, ax2 = plt.subplots()
    #ax2.hist(track_df.hr10)
    #fig2.show()
    return trimp

#%%
if __name__ == "__main__":  
    f = r"C:\Users\Matthew\Documents\Dropbox\gpx\strava_export_20161224"   
    print dt.datetime.now()
    fmt = '%Y-%m-%d %H:%M:%S'
    last_mod_date = dt.datetime.strptime('2017-01-13 22:36:49', fmt)
    
    files = []

    for root, dirnames, filenames in os.walk(f):
        for name in filenames:
            subDirPath = os.path.join(root, name)
            if (dt.datetime.fromtimestamp(os.path.getmtime(subDirPath)) > last_mod_date):
                if '<gpxtpx:hr>' in open(subDirPath).read():
                    files += [subDirPath]
                    print name
    #%% 
    cols = ['datetime','activity_id','trimp']
    df = pd.DataFrame(columns = cols)
    print 'processing files'
    for fname in files:
        print '...'+fname
        gpx = GPX(fname)
        activity_id = 1
        trimp = calc_trimp(gpx, 'm', 185, 45)
        datetime = gpx.start_time()
        datetime = dt.datetime.strftime(datetime,"%Y-%m-%d %H:%M:%S")
        df = df.append(pd.DataFrame([[datetime,int(activity_id),trimp]], columns = cols))
        
    engine = create_engine('mssql+pyodbc://athelite')
    df=df.set_index(pd.DatetimeIndex(df.datetime))
    df = df.resample('1D').sum()
    df = df.fillna(0)
    df.to_sql('tbl_trimp', engine, if_exists = 'append', index = True, index_label = 'datetime', schema = 'athelite.dbo')