import pandas as pd
import pymysql
import time

url = 'https://tw.stock.yahoo.com/d/s/dividend_{}.html'
conn = pymysql.connect(host='127.0.0.1',user='root',password='842369',db='stock')

if __name__ == '__main__':    
    cursor = conn.cursor()     
    sql = "SELECT distinct(code) \
           FROM company_map \
           where code not in ( \
               select code \
               from dividend)"
    cursor.execute(sql)
    codes = []
    for row in cursor:
        codes.append(row[0])
    for code in codes:        
        print (code)
        # Encode this website by Big5
        try:
            df = pd.read_html(url.format(code),encoding="Big5")[1]
        except:
            continue
        # Delete first line, delete chinese columns
        df = df.drop(0).reset_index(drop=True) 
        
        for index, row in df.iterrows():
            if index == 0 or index ==1: 
                continue
            try:                
                sql = "INSERT INTO dividend (`code`,`year`,`cash`,`allotment`,`total`) VALUES (%s,%s,%s,%s,%s)"
                val = (code,row[0],row[1],row[4],row[5])
                cursor.execute(sql, val)
                conn.commit()
            except Exception as e:
                print (e)
        time.sleep(5)



