import urllib.request
import time
import pymysql
from bs4 import BeautifulSoup

url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb04?'\
      'encodeURIComponent=1&step=1&firstin=1&off=1&TYPEK=sii&year=102&season=01'
conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')
years = [2015]
seasons = ["1","2","3","4"]

def indivisualSeason(cursor, year, season):
    sql = "select code, year, season, eps from eps where year={}".format(year)
    cursor.execute(sql)
    emap = dict()
    for row in cursor: 
        code = row[0]
        year = row[1] 
        season = row[2]
        eps = row[3]     
        if code not in emap:
            emap[code] = dict()
        if year not in emap[code]:
            emap[code][year] = dict()
        if season not in emap[code][year]:
            emap[code][year][season] = dict()
        emap[code][year][season] = eps
    for code in emap:
        for year in emap[code]:
            for season in emap[code][year]:
                try:
                    if season == 'Q1':
                        _season = season
                        _eps = emap[code][year][season]
                    else:
                        _season = 'Q' + str(int(season[1])-1)                
                        _eps = emap[code][year][season] - emap[code][year][_season]
                    sql = "INSERT INTO _eps (`code`,`year`,`season`,`eps`) VALUES (%s,%s,%s,%s)"
                    val = (code,year,season,_eps)
                    cursor.execute(sql, val)
                    conn.commit() 
                except Exception as e:
                    print (e)
                    continue                
        print (code)    

if __name__ == '__main__': 
    cur = conn.cursor()
    for year in years:    
        for season in seasons:
            url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb04?' + \
           'encodeURIComponent=1&step=1&firstin=1&off=1&TYPEK=sii&year='+str(year-1911) + \
           '&season=0' + season
            response = urllib.request.urlopen(url)
            html = response.read()
            sp = BeautifulSoup(html.decode('utf8'), "lxml")
            tbls=sp.find_all('table',attrs={ 'class' : "hasBorder"})

            for tbl in tbls:
                ths=tbl.find_all('th')
                i=0
                j=0
                for th in ths:
                    i=i+1
                    if "每股盈餘"  in th.get_text():
                        j=i   
                trs=tbl.find_all('tr',attrs={ 'class' : "even"})                
                for tr in trs:
                    try:
                        tds=tr.find_all('td')
                        sql = "insert into eps (`code`,`year`,`season`,`eps`) values (%s,%s,%s,%s)"
                        val = (tds[0].get_text(), str(year), 'Q'+season, tds[j-1].get_text())    
                        cur.execute(sql, val)             
                    except Exception as e:
                        print (e)   
                    conn.commit()  
            time.sleep(10)
            '''計算每季的eps,原本Q4=Q1+Q2+Q3+Q4'''
            indivisualSeason(cur,year,season)


