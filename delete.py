import urllib.request
import time
import pymysql
from bs4 import BeautifulSoup

conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

if __name__ == '__main__':
    try: 
        cursor = conn.cursor()
        sql = "DELETE FROM `stock`.`ma` WHERE `date` >= '20190710' "
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        print (e)
