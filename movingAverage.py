import pymysql
import requests
import datetime
import time 
from twstock import Stock
conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')
'''
def newsParser(conn,MAX_COUNT):
    cursor = conn.cursor()     
    origin_url = 'https://fnc.ebc.net.tw/Search/Result?type=keyword&value='
    base_url = 'https://fnc.ebc.net.tw'       
    MAX_FAIL_COUNT = 3 #新增三次失敗就放棄  
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
'''
if __name__ == '__main__': 
    cursor = conn.cursor()     
    code_list = []     
    sql = "SELECT distinct(code) FROM own"
    cursor.execute(sql)
    for row in cursor:
        code = row[0]
        stock = Stock(code)
        week_ma = stock.moving_average(stock.price, 5)
        month_ma = stock.moving_average(stock.price, 20)
        season_ma = stock.moving_average(stock.price, 60)
        print (code, week_ma[-1], month_ma[-1], season_ma[-1])
        time.sleep(10) 



