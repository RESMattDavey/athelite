# -*- coding: utf-8 -*-
"""
Created on Fri Dec 02 23:17:17 2016

@author: Matthew
"""

import pandas as pd
from sqlalchemy import create_engine
import datetime as dt

def insert_measurements(fname, sheet_name ,user_id, db):  
    df= pd.read_excel(fname, sheetname = sheet_name,index_col = 'timestamp')
    cols = ['user_id','timestamp','measurement','meas_value','meas_unit']
    measdf = pd.DataFrame(columns = cols)
    for col in df.columns:
        for idx in df[col].index:
            measdf = measdf.append(pd.DataFrame([[user_id,idx,col.split(";")[0], df.ix[idx][col], col.split(";")[1]]], columns = cols),ignore_index = True)
    measdf=measdf[~measdf.meas_value.isnull()]
    print 'Length of input table: %d' % len(measdf)
    
    
    engine = create_engine(db)
    con = engine.connect()
    p = con.execute('SELECT max([measurement_id]) FROM  athelite.dbo.tbl_measurements')
    max_id = p.fetchall()[0][0]   
    

    measdf.timestamp = measdf.timestamp.dt.strftime('%Y-%m-%d %H:%M')
    measdf.user_id = measdf.user_id.astype(int)
    measdf['measurement_id'] = measdf.index
    measdf['measurement_id'] = measdf['measurement_id'].astype(int)+max_id+1
    measdf[['measurement_id','user_id', 'timestamp', 'measurement', 'meas_value', 'meas_unit']].to_sql('tbl_measurements', engine, if_exists = 'append', schema = 'athelite.dbo', index = False)
    
    
    p = con.execute('SELECT * FROM  athelite.dbo.tbl_measurements')
    df1 = pd.DataFrame(p.fetchall())
    
    del_qry = """delete from athelite.[dbo].[tbl_measurements] where measurement_id in 
    (SELECT 
    	  max(measurement_id) as min_dup_id
      FROM [athelite].[dbo].[tbl_measurements]
       group by [user_id]
          ,[timestamp]
          ,[measurement] 
      having count([user_id])  > 1)"""
      
    con.execute(del_qry)
    p = con.execute('SELECT * FROM  athelite.dbo.tbl_measurements')
    df2 = pd.DataFrame(p.fetchall())
    print 'number of records removed as duplicates: %d' % (len(df1) - len(df2))

if __name__ == '__main__':
    fname = "C:\Users\Matthew\Documents\Dropbox\Documents\mat fan tracker.xlsx"
    db = 'mssql+pyodbc://athelite'
    users = [1,2]
    for user_id in users:
        if user_id == 1: sheet_name = 'Matt'
        if user_id == 2: sheet_name = 'Fan'
        insert_measurements(fname, sheet_name,user_id, db)
        
 
        

        
        

        