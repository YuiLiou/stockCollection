import pymysql
import requests
import pandas as pd
import numpy as np
import datetime
import twstock
from io import StringIO

conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')
req_url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date='     
    
if __name__ == '__main__':    
    try:
        cursor = conn.cursor()
        datestr = "20190808"
        r = requests.post(req_url + datestr + '&type=ALL')    
        df = pd.read_csv(StringIO("\n".join([i.translate({ord(c): None for c in ' '}) 
                         for i in r.text.split('\n') 
                         if len(i.split('",')) == 17 and i[0] != '='])), header=0)
        twDict = twstock.codes
        for index, row in df.iterrows(): 
            try:
                code = row['證券代號']
                sql = "insert into company_map (`code`,`type`,`company`,`start`,`market`,`grp`) \
                       values (%s,%s,%s,%s,%s,%s)"
                twTuple = twDict[code]
                # tuple index: type, code, name, isin, start, market, group
                val = (code, twTuple[0],twTuple[2],twTuple[4],twTuple[5],twTuple[6])
                cursor.execute(sql, val)
                conn.commit()     
            except Exception as e:
                print (e)   
    except Exception as e:
        print (e)





