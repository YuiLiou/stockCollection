# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import pandas as pd
import scrapy
import requests
import pymysql
import time
from htmltable_df.extractor import Extractor
conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

def start_requests(cursor, beginDate, endDate):
    url = 'https://www.tdcc.com.tw/smWeb/QryStockAjax.do'
    for date in pd.date_range(beginDate, endDate, freq='W-FRI')[::-1]:
        scaDate = '{}{:02d}{:02d}'  .format(date.year, date.month, date.day)
        date    = '{}/{:02d}/{:02d}'.format(date.year, date.month, date.day)
        sql = "SELECT code " \
              "FROM own " \
              "where code not in ( " \
              "    select code " \
              "    from share_ratio " \
              "    where 1=1 " \
              "    and date = '" + scaDate + "'" \
              ") " \
              "group by code "
        cursor.execute(sql)
        code_list = list()
        for row in cursor:
            code_list.append(row[0])
        try:
            for code in code_list:
                payload = {
                    'scaDates': scaDate,
                    'scaDate': scaDate,
                    'SqlMethod': 'StockNo',
                    'StockNo': code,
                    'radioStockNo': code,
                    'StockName': '',
                    'REQ_OPR': 'SELECT',
                    'clkStockNo': code,
                    'clkStockName': ''
                } 
                html=requests.post(url,data=payload).text
                data=Extractor(html,'table.mt:eq(1)').df(1)
                for index, row in data.iterrows():        
                    if index == 15:
                        continue
                    sql = "INSERT INTO share_ratio (`date`,`code`,`rank`,`number`,`person`,`rate`) \
                           VALUES (%s,%s,%s,%s,%s,%s)"
                    val = (scaDate,code,row['持股/單位數分級'],row['股　　數/單位數'],row['人　　數'],row['占集保庫存數比例 (%)'])
                    cursor.execute(sql, val)
                    conn.commit()
                    print (code, scaDate, row['持股/單位數分級'],row['股　　數/單位數'],row['人　　數'],row['占集保庫存數比例 (%)'])
                time.sleep(3)                 
        except Exception as e:
            print (e)
            
if __name__ == '__main__':
    cur = conn.cursor()
    endDate   =  (datetime.today() - timedelta(days=14)).strftime("%Y/%m/%d")
    startDate =  (datetime.today() - timedelta(days=365)).strftime("%Y/%m/%d")
    start_requests(cur,startDate,endDate)


    


