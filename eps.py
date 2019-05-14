import urllib.request
import time
import pymysql
from bs4 import BeautifulSoup

url = 'http://mops.twse.com.tw/mops/web/ajax_t163sb04?'\
      'encodeURIComponent=1&step=1&firstin=1&off=1&TYPEK=sii&year=102&season=01'
conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')
years = [2019]
seasons = ["1"]

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


