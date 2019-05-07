import pymysql
from news import newsParser
from legals import legalsParser
from price import priceParser

conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')
cursor = conn.cursor()  

if __name__ == '__main__':      
    newsParser(cursor,20)
    legalsParser(cursor, 2)
    priceParser(cursor,2)
