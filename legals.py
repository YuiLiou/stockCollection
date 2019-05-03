import twstock
import pymysql
import requests
import pandas as pd
import numpy as np
import time 
import datetime
from io import StringIO

conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')
cursor = conn.cursor()         
n_days = 25
date = datetime.datetime.now()

def insertIntoDB(df):
    for index, row in df.iterrows(): 
        try:       
            foreign = float(row['外陸資買賣超股數(不含外資自營商)'].replace(',',''))
            dealer = float(row['自營商買賣超股數'].replace(',',''))
            investment = float(row['投信買賣超股數'].replace(',',''))
            total = float(row['三大法人買賣超股數'].replace(',',''))        
            sql = "INSERT INTO legals (`code`,`date`,`foreigner`,`dealer`,`investment`,`total`) VALUES (%s,%s,%s,%s,%s,%s)"
            val = (row['證券代號'],datestr,foreign,dealer,investment,total)
            cursor.execute(sql, val)
            conn.commit()
        except Exception as e:
            print (e)

if __name__ == '__main__':           
    
    for i in range(n_days):
        if date.weekday() in [0,1,2,3,4]:
            try:
                datestr = date.strftime("%Y%m%d")
                r = requests.get('http://www.tse.com.tw/fund/T86?response=csv&date='+datestr+'&selectType=ALLBUT0999')   
                df = pd.read_csv(StringIO(r.text), header=1).dropna(how='all', axis=1).dropna(how='any')
                insertIntoDB(df)                                
            except Exception as e:
                print (e) 
        date -= datetime.timedelta(days=1)
        time.sleep(10)
