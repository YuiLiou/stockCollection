import pymysql
from news import newsParser
from legals import legalsParser
from price import priceParser

conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

if __name__ == '__main__':      
    priceParser(conn,1)   
    legalsParser(conn,1)
    newsParser(conn,20)
