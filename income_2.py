import time
import pymysql
import requests
import pandas as pd
import numpy as np
import math

years = [2019]
seasons = [2]
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
            df = financial_statement(year,season,'綜合損益彙總表')        
            col_names = list(df.columns.values)
            for index, row in df.iterrows():
                for col in col_names:
                    try:
                        if math.isnan(float(row[col])):
                            continue
                        sql = "insert into income_2 (`year`,`season`,`code`,`col_name`,`value`) "\
                              "values(%s,%s,%s,%s,%s) "
                        val = (year, 'Q'+str(season), row['公司代號'], col, row[col])
                        cur.execute(sql, val)
                    except Exception as e:
                        print (e)
            conn.commit()
            time.sleep(10)    


