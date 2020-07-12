# -*- coding: utf-8 -*-
#from datetime import datetime, timedelta
import datetime
import pandas as pd
import scrapy
import requests
import pymysql
import time
from htmltable_df.extractor import Extractor
#conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

def isNumer(user_input):
    try:
        val = int(user_input)
        return True
    except ValueError:
        try:
            val = float(user_input)
            return True
        except ValueError:
            return False

def start_requests(conn, beginDate, endDate):
    cursor = conn.cursor()
    print (beginDate)
    print (endDate)
    url = 'https://www.tdcc.com.tw/smWeb/QryStockAjax.do'
    for date in pd.date_range(beginDate, endDate, freq='W-FRI')[::-1]:
#    for date in pd.date_range(beginDate, endDate, freq='W-WED')[::-1]:
#    for date in pd.date_range(beginDate, endDate, freq='W-THU')[::-1]:
#    for date in pd.date_range(beginDate, endDate, freq='W-SAT')[::-1]:
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
                headers = {"User-Agent" : "User-Agent:Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0;"}          
                html=requests.post(url,data=payload, headers=headers).content.decode('big5')
                data=Extractor(html,'table.mt:eq(1)').df(1)
                for index, row in data.iterrows():        
                    if index == 15:
                        continue
                    sql = "INSERT INTO share_ratio (`date`,`code`,`rank`,`number`,`person`,`rate`) \
                           VALUES (%s,%s,%s,%s,%s,%s)"
                    if isNumer(row['股　　數/單位數']):
                        val = (scaDate,code,row['持股/單位數分級'],row['股　　數/單位數'],row['人　　數'],row['占集保庫存數比例 (%)'])
                        cursor.execute(sql, val)
                        conn.commit()
                        print (code, scaDate, row['持股/單位數分級'],row['股　　數/單位數'],row['人　　數'],row['占集保庫存數比例 (%)'])
                time.sleep(3)                 
        except Exception as e:
            print (e)
            
def shareRatioParser(conn):
    cur = conn.cursor()
    # 最近更新日期 ---------------------------------------------------------------------
    sql = "select max(date) " \
          "from share_ratio " \
          "where code in " \
          "( " \
          "  select max(code) " \
          "  from own " \
          ") "
    cur.execute(sql)
    for row in cur:
        beginDate = datetime.datetime.strptime(row[0],"%Y%m%d").strftime("%Y/%m/%d")
    #beginDate = datetime.datetime(2020,4,29).strftime("%Y/%m/%d")
    # --------------------------------------------------------------------------------
    #endDate   = datetime.datetime(2020,5,1).strftime("%Y/%m/%d")
    endDate = datetime.datetime.now().strftime("%Y/%m/%d")
    start_requests(conn,beginDate,endDate)


    


