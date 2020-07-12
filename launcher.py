import pymysql
from news import newsParser
from legals import legalsParser
from price import priceParser
from share_ratio import shareRatioParser

conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

if __name__ == '__main__': 
    print ("=================集保股權異動=================")
    shareRatioParser(conn)
    print ("=================股價=================")    
    priceParser(conn)   
    print ("=================法人=================")
    legalsParser(conn)
    print ("=================新聞=================")
    newsParser(conn,20)
