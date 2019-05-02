import twstock
import pymysql
import requests
import pandas as pd
import numpy as np

year = 2017
season = 4
db_season = str(year) + 'Q' + str(season)

conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

def financial_statement(year, season, type='綜合損益彙總表'):
    if year >= 1000:
        year -= 1911
    if type == '綜合損益彙總表':
        url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb04'
    elif type == '資產負債彙總表':
        url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb05'
    elif type == '營益分析彙總表':
        url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb06'
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
    # 公司代碼對照
    try:
        cur = conn.cursor()        
        df = financial_statement(year,season,'營益分析彙總表')        
        for index, row in df.iterrows():
            try:
                sql = "insert into company_map (`company`,`code`) values (%s,%s)"
                val = (row['公司名稱'],row['公司代號'])
                cur.execute(sql, val)
                conn.commit()
            except:
                print ('insert error')
    except Exception as e:
        print (e)
