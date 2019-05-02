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
n_days = 2
date = datetime.datetime.now()

def insertIntoDB(df):
    for index, row in df.iterrows(): 
        try:       
            today = float(row['收盤價'].replace(',',''))
            if row['漲跌(+/-)'] == '+':
                change = float(row['漲跌價差'])                        
            else:
                change = -1*float(row['漲跌價差'])
            yesterday = today - change
            moving = round((change/yesterday*100),2) 
            volume = float(row['成交股數'].replace(',',''))
            PE = float(row['本益比'].replace(',',''))
            sql = "INSERT INTO prices (`code`,`date`,`price`,`moving`,`change`,`volume`,`PE`) VALUES (%s,%s,%s,%s,%s,%s,%s)"
            val = (row['證券代號'],datestr,today,moving,change,volume,PE)
            cursor.execute(sql, val)
            conn.commit()
        except Exception as e:
            print (e)

if __name__ == '__main__':           
    
    for i in range(n_days):
        if date.weekday() in [0,1,2,3,4]:
            try:
                datestr = date.strftime("%Y%m%d")
                r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALL')    
                df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) 
                                    for i in r.text.split('\n') 
                                     if len(i.split('",')) == 17 and i[0] != '='])), header=0)
                insertIntoDB(df)                
            except Exception as e:
                print (e) 
        date -= datetime.timedelta(days=1)
        time.sleep(10)
