import pymysql
import requests
import datetime
import time 
from bs4 import BeautifulSoup



def newsParser(conn,MAX_COUNT):
    cursor = conn.cursor()     
    origin_url = 'https://fnc.ebc.net.tw/Search/Result?type=keyword&value='
    base_url = 'https://fnc.ebc.net.tw'       
    MAX_FAIL_COUNT = 1 #新增1次失敗就放棄  
    code_list = []     
    sql = "SELECT distinct(code) FROM own"
    cursor.execute(sql)
    for row in cursor:
        code_list.append(row[0])
    for code in code_list:
        print (code)
        new_url = origin_url + code
        res = requests.get(new_url)
        soup = BeautifulSoup(res.text, 'html.parser')
        count = 0
        fail_count = 0
        for link in soup.find(class_="fncnews-list-box").find_all('a'):
            date = link.find(class_='small-gray-text').text.replace('(','').replace(')','')
            title = link.find('span').text
            url = base_url + link['href']
            if title.find('◆') == -1: # 不看法人買賣超新聞           
                try:                
                    sql = "INSERT INTO news (`code`,`date`,`title`,`url`,`logTime`) VALUES (%s,%s,%s,%s,%s)"
                    val = (code,date,title,url,datetime.datetime.now())
                    cursor.execute(sql, val)
                    conn.commit()
                    if count == MAX_COUNT: 
                        break
                    else:
                        count = count + 1
                    print (date,title,url)
                except:
                    if fail_count == MAX_FAIL_COUNT:
                        break
                    else:
                        fail_count = fail_count + 1                
                        print ('insert failed')
        time.sleep(3) 
