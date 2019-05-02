import twstock
import pymysql
import requests
import pandas as pd
import numpy as np
from io import StringIO
import time
import math

year = 2018
months = [1,2,3,4,5,6,7,8,9,10,11,12]
conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

def monthly_report(year, month):
    
    # 假如是西元，轉成民國
    if year > 1990:
        year -= 1911
    
    url = 'http://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(year)+'_'+str(month)+'_0.html'
    if year <= 98:
        url = 'http://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(year)+'_'+str(month)+'.html'
    
    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    
    # 下載該年月的網站，並用pandas轉換成 dataframe
    r = requests.get(url, headers=headers)
    r.encoding = 'big5'
    dfs = pd.read_html(StringIO(r.text))
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
    
    if 'levels' in dir(df.columns):
        df.columns = df.columns.get_level_values(1)
    else:
        df = df[list(range(0,10))]
        column_index = df.index[(df[0] == '公司代號')][0]
        df.columns = df.iloc[column_index]
    
    df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
    df = df[~df['當月營收'].isnull()]
    df = df[df['公司代號'] != '合計']
    
    # 偽停頓
    time.sleep(5)

    return df
if __name__ == '__main__':    
    # 綜合損益總表
    try:
        cur = conn.cursor() 
        for month in months:       
            df = monthly_report(year,month)
            for index, row in df.iterrows():
                sql = "insert into monthly (`code`,`month`,`current`,`Yearly`,`YoY`,`Yearly_YoY`) values (%s,%s,%s,%s,%s,%s)"
                if isinstance(row['去年同月增減(%)'],str):
                    YoY = row['去年同月增減(%)']
                else:
                    YoY = 0
                if isinstance(row['前期比較增減(%)'],str):
                    Yearly_YoY = row['前期比較增減(%)']
                else:
                    Yearly_YoY = 0
                s_month = str(year) + str.zfill(str(month),2)
                val = (row['公司代號'],s_month,row['當月營收'],row['當月累計營收'],YoY,Yearly_YoY)    
                cur.execute(sql, val)               
            conn.commit()       
    except Exception as e:
        print (e)
