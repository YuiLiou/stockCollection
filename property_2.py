import time
import pymysql
import requests
import pandas as pd
import numpy as np
import math

yearList = [103,104,105,106,107,108]
seasonList = [1,2,3,4]
conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

def financial_statement(year, season, code):
    if year >= 1000:
        year -= 1911
    url = 'https://mops.twse.com.tw/mops/web/ajax_t164sb03'
    form_data = {
        'encodeURIComponent':1,
        'step':1,
        'firstin':1,
        'off':1,
        'queryName':'co_id',
        'inpuType':'co_id',
        'TYPEK':'all',
        'isnew':'false',
        'co_id':code,
        'year':str(year),
        'season':str(season)
    }
    r = requests.post(url,form_data)
    html_df = pd.read_html(r.text)[1].fillna("")
    return html_df

def insertRows(stock_df, cur):
    for index, row in stock_df.iterrows():         
        for i in range(len(row)):
            if not row[i]:
                row[i] = ""
        try:
            # 標題
            if index == 2:
                sql = "insert into property_2 " \
                      "(`year`,`season`,`code`,`col_name`,`col_index`,`v1`,`v2`,`v3`,`v4`,`v5`,`v6`) " \
                      "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                val = (yearStr,seasonStr,code,row[0],index,row[1],"%",row[2],"%",row[3],"%")
                cur.execute(sql, val)
            # 去年全年+同期
            elif index > 3 and len(row) > 5:
                sql = "insert into property_2 " \
                      "(`year`,`season`,`code`,`col_name`,`col_index`,`v1`,`v2`,`v3`,`v4`,`v5`,`v6`) " \
                      "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                val = (yearStr,seasonStr,code,row[0],index,row[1],row[2],row[3],row[4],row[5],row[6])
                cur.execute(sql, val)
            # 去年同期
            elif index > 3:    
                sql = "insert into property_2 " \
                      "(`year`,`season`,`code`,`col_name`,`col_index`,`v1`,`v2`,`v3`,`v4`) " \
                      "values(%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                val = (yearStr,seasonStr,code,row[0],index,row[1],row[2],row[3],row[4])
                cur.execute(sql, val)
        except Exception as e:
            print (e)    
    conn.commit()             

if __name__ == '__main__':    
    cur = conn.cursor()
    for year in yearList :
        for season in seasonList:            
            seasonStr = 'Q'+str(season)
            yearStr = str(year+1911)
            sql = "SELECT code " \
                  "FROM own " \
                  "where code not in ( " \
                  "    select code " \
                  "    from property_2 " \
                  "    where 1=1 " \
                  "    and year = '" + yearStr + "'" \
                  "    and season = '" + seasonStr + "'" \
                  ") " \
                  "group by code "
            cur.execute(sql)
            code_list = list()
            for row in cur:
                code_list.append(row[0])
            for code in code_list:
                try:
                    stock_df = financial_statement(year,season,code)        
                    insertRows(stock_df, cur)
                    print (code, ' complete.')         
                except Exception as e:
                    print (code, 'Fail!:', e)
                finally:
                    time.sleep(10)     


