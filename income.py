import time
import pymysql
import requests
import pandas as pd
import numpy as np

years = [2019]
seasons = [3]
conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

def financial_statement(year, season, type='綜合損益彙總表'):
    if year >= 1000:
        year -= 1911
    if type == '綜合損益彙總表':
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    elif type == '資產負債彙總表':
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb05'
    elif type == '營益分析彙總表':
        url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb06'
    else:
        print('type does not match')
    r = requests.post(url, {
        'encodeURIComponent':1,
        'step':1,
        'firstin':1,
        'off':1,
        'TYPEK':'sii',
        'year':str(year),
        'season':str(season),
    })    
    r.encoding = 'utf8'
    dfs = pd.read_html(r.text)   
    for i, df in enumerate(dfs):
        df.columns = df.iloc[0]
        dfs[i] = df.iloc[1:]        
    df = pd.concat(dfs).applymap(lambda x: x if x != '--' else np.nan)
    df = df[df['公司代號'] != '公司代號']
    df = df[~df['公司代號'].isnull()]
    return df

if __name__ == '__main__':    
    # 綜合損益總表
    for year in years:
        for season in seasons:
            cur = conn.cursor()        
            df = financial_statement(year,season,'營益分析彙總表')        

            for index, row in df.iterrows():
                try:                    
                    sql = "insert into income " \
"(`code`,`year`,`season`,`grossRate`,`operatingRate`,`operatingIncome`,`beforeTaxRate`,`afterTaxRate`)" \
"values (%s,%s,%s,%s,%s,%s,%s,%s)"
                    val = (row['公司代號'],year,'Q'+str(season),row['毛利率(%)(營業毛利)/(營業收入)'],row['營業利益率(%)(營業利益)/(營業收入)'],row['營業收入(百萬元)'],row['稅前純益率(%)(稅前純益)/(營業收入)'],row['稅後純益率(%)(稅後純益)/(營業收入)'])
                    cur.execute(sql, val)                    
                except Exception as e:
                    print (e)    
            conn.commit()
            print (year, season)
            time.sleep(10)    

