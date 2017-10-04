# -*- coding: utf-8 -*-
"""
Created on Thu Dec 01 00:32:15 2016

@author: Matthew
"""

from sqlalchemy import create_engine
import pandas as pd
from matplotlib import pyplot as plt
import pyodbc

        
def calc_1rm(df):
    df.loc[df['reps']>10,'reps'] = 10
    df['landers'] = df['mass']*100 / (101.3-(2.67123 * df['reps']))
    df['bryzcki'] =  df['mass'] / (1.0278 - (0.0278 * df['reps']))
    df['baechle'] = df['mass'] * (1 + (0.033 * df['reps']))
    df['epley'] = (df['mass'] * df['reps'] * 0.033) + df['mass']
    df['max_median'] = df.median(axis =1)
    df = df[['set_id','max_median']]
    return df
    
if __name__ == '__main__':
    engine = create_engine('mssql+pyodbc://athelite')
    query = """SELECT * from athelite.dbo.tbl_sets where max_median is NULL"""
    df = pd.read_sql(query,engine)
    cols =  df.columns
    #df = df.set_index('set_id')
    df_1rm = calc_1rm(df)
    #print df
    df = df[cols]
    df.to_sql('tbl_sets', engine, if_exists = 'replace', schema = 'athelite.dbo', index = False)