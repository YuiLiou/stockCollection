import pymysql
import requests
import pandas as pd
import numpy as np
import time 
import datetime
from io import StringIO

def insertIntoDB(df,conn,datestr):
    cursor = conn.cursor()  
    for index, row in df.iterrows(): 
        try:
            code = row['證券代號']
            #-------------------------- 收盤價 ----------------------------------                   
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
            val = (code,datestr,today,moving,change,volume,PE)
            cursor.execute(sql, val)
            conn.commit()            
            #-------------------------- moving average ----------------------------------
            for ma in [5,20,60]:
                sql = "select avg(p.price) \
                       from (select code , price \
                             from prices \
                             where 1=1 \
                             and date <= {} \
                             and code = {} \
                             order by date desc \
                             limit 0,{}) p \
                       group by p.code".format(datestr,code,ma) 
                cursor.execute(sql)
                for row in cursor:                    
                    sql2 = "insert into ma (`code`,`date`,`span`,`value`) values (%s,%s,%s,%s)" 
                    val2 = (code, datestr, ma, row[0])
                    cursor.execute(sql2, val2)
                    conn.commit()             
        except Exception as e:
            print (e)

def priceParser(conn):           
    req_url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date='     
    
    #//////////////////////////////// start date ////////////////////////////////
    cursor = conn.cursor() 
    sql = "select date from prices order by date desc limit 0,1 "
    cursor.execute(sql)
    start_date = ""
    for row in cursor:
        start_date = row[0]

    #//////////////////////////////// 更新前幾天股價 ////////////////////////////////     
    date = datetime.datetime.now()
    while date.strftime("%Y%m%d") != start_date:
        if date.weekday() in [0,1,2,3,4]:
            try:
                datestr = date.strftime("%Y%m%d")
                r = requests.post(req_url + datestr + '&type=ALL')    
                df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) 
                                    for i in r.text.split('\n') 
                                     if len(i.split('",')) == 17 and i[0] != '='])), header=0)
                insertIntoDB(df,conn,datestr)
                print (datestr,'股價更新')                
            except Exception as e:
                print (e) 
        date -= datetime.timedelta(days=1)
        time.sleep(10)


