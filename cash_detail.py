import requests
import pandas as pd
import pymysql
import time

# 爬取目標網站
year = 108
season = 2

BalanceSheetURL = "http://mops.twse.com.tw/mops/web/ajax_t164sb03";      # 資產負債表
ProfitAndLoseURL = "https://mops.twse.com.tw/mops/web/ajax_t164sb04";    # 損益表
CashFlowStatementURL = "https://mops.twse.com.tw/mops/web/ajax_t164sb05"; # 現金流量表

conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

def crawl_financial_Report(url, code):    
    form_data = {
        'encodeURIComponent':1,
        'step':1,
        'firstin':1,
        'off':1,
        'queryName':'co_id',
        'inpuType':'co_id',
        'isnew':False,
        'TYPEK':'all',
        'co_id':code,
        'year': year,
        'season': season
    }
    r = requests.post(url,form_data)
    html_df = pd.read_html(r.text)[1].fillna("")
    return html_df

if __name__ == '__main__':    
    cur = conn.cursor() 
    company_list = list()
    seasonStr = 'Q'+str(season)
    sql = "SELECT code FROM own group by code " 
    cur.execute(sql)
    for row in cur:
        company_list.append(row[0])
    for code in company_list:
        try:
            stock_df = crawl_financial_Report(CashFlowStatementURL, code)       # 損益表
            for index, row in stock_df.iterrows():            
                for i in range(len(row)):
                    if not row[i]:
                        row[i] = ""
                if index > 3 or index == 2:
                    sql = "insert into cash_detail " \
                          "(`year`,`season`,`code`,`col_name`,`index`,`v1`,`v2`) " \
                          "values(%s,%s,%s,%s,%s,%s,%s) "
                    val = (year,seasonStr,code,row[0],index,row[1],row[2])
                    cur.execute(sql, val)
            conn.commit()               
        except Exception as e:
            print (e)
        finally:
            print (code , ' complete.')
            time.sleep(10)

