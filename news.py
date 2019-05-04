import pymysql
import requests
import datetime
import time 
from bs4 import BeautifulSoup

origin_url = 'https://fnc.ebc.net.tw/Search/Result?type=keyword&value='
base_url = 'https://fnc.ebc.net.tw'
conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')
cursor = conn.cursor()         

if __name__ == '__main__':      
    code_list = []     
    sql = "SELECT distinct(code) FROM own"
    cursor.execute(sql)
    for row in cursor:
        code_list.append(row[0])
    for code in code_list:
        new_url = origin_url + code
        res = requests.get(new_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        counter = 0
        for link in soup.find(class_="fncnews-list-box").find_all('a'):
            date = link.find(class_='small-gray-text').text.replace('(','').replace(')','')
            title = link.find('span').text
            url = base_url + link['href']
            try:
                print (date,title,url)
                sql = "INSERT INTO news (`code`,`date`,`title`,`url`,`logTime`) VALUES (%s,%s,%s,%s,%s)"
                val = ('2330',date,title,url,datetime.datetime.now())
                cursor.execute(sql, val)
                conn.commit()
                if counter == 10: 
                    break
                else:
                    counter = counter + 1
            except:
                print ('insert failed')
        time.sleep(3) 
