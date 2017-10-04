# -*- coding: utf-8 -*-
"""
Created on Tue Oct 03 22:33:46 2017

@author: Matthew
"""
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
import argparse


def save_meas(value,unit,measurement,user):
    success = False
    try:
        qry = """SELECT top 1 measurement_id from tbl_measurements where measurement_id is not null order by [measurement_id] desc"""
        engine = create_engine("mssql+pyodbc://@TEAMSOLVEIG\TS1/athelite?driver=SQL+Server+Native+Client+10.0")
        top_id = pd.read_sql(qry,engine).values[0][0]
        
        new_id = top_id + 1
        cols = ['measurement_id', 'user_id','timestamp','measurement','meas_value','meas_unit']
        df = pd.DataFrame([[new_id,user,dt.datetime.now(),measurement,value,unit]], columns = cols)
        df.to_sql('tbl_measurements',engine,schema = 'dbo',if_exists = 'append',index= False)
        success = True
    except: pass
    return success

if __name__ == '__main__':     
    parser = argparse.ArgumentParser()
    parser.add_argument('measurement')
    parser.add_argument('value', type = float)
    parser.add_argument('unit')
    parser.add_argument('user', type = int)    
    args = parser.parse_args()
    
    unit = args.unit
    user = args.user
    value = args.value
    measurement = args.measurement
    saved = save_meas(value,unit,measurement,user)
    if saved:
        print 'measurement saved successfully'