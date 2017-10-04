# -*- coding: utf-8 -*-
"""
Created on Mon Dec 05 22:18:02 2016

@author: Matthew
"""
import pandas as pd
from sqlalchemy import create_engine
import datetime as dt
from matplotlib import pyplot as plt
if __name__ == '__main__':
    db = 'mssql+pyodbc://athelite'
    users = [1,2]
 
    engine = create_engine(db)
    df = pd.read_sql('SELECT user_id, timestamp, measurement, meas_value, meas_unit FROM  athelite.dbo.tbl_measurements',engine)
    df = df.set_index('timestamp')   
    df = df[df.measurement == 'Mass']
    df = df.pivot(None,'user_id','meas_value')
    df = df.interpolate(axis =0)
    df.plot()
    plt.ylim(0,100)