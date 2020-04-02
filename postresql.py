import psycopg2
import os
import csv
import codecs
import time
import sys
import difflib
import matplotlib.pyplot as plt
from passwordFile import password

db=psycopg2.connect(dbname='thesales',user='postgres',host='localhost', password=password )
cur=db.cursor()
db.autocommit=False

def createTableValues(headers):
    d=[f'{v} TEXT,' for v in headers]
    #remove last column in last list position
    d[len(d)-1]=d[len(d)-1][:-1]
    s=''.join(d)
    return s

def getTableName(readerObject):
    headers=readerObject.fieldnames
    tableValues=createTableValues(headers)
    if headers[4]=='PaymentName':
        return 'payments',tableValues
    elif headers[4]=='Description':
        return 'sales',tableValues
    else:
        print('unknow file')
        sys.exit()

path='E:\PythonCoding\Reports'
path2='E:\PythonCoding\Reports2'

def loadFolderSlow(path):
    ''' Fill up database with line by line commit'''
    cur.execute('SET synchronous_commit TO off;') 
    directory=os.fsencode(path)
    files = (str(f).split("'")[1] for f in os.listdir(directory))
    for file in files:
        o=csv.DictReader(codecs.open(f'{path}\{file}','r',encoding='utf-8'))
        tableName=getTableName(o)
        cur.execute(f'CREATE TABLE IF NOT EXISTS {tableName} (BillheaderID TEXT,CheckitemID TEXT);')
        cur.execute(f''' ALTER TABLE {tableName} SET UNLOGGED; ''')
        dataLeft=True
        while dataLeft:
            try:
                values=list(next(o).values())
                query=f'INSERT INTO {tableName} VALUES (%s)'
                cur.execute(query,(values,))
            except StopIteration:
                dataLeft=False
        print(f'finished {file}')
    cur.execute(f''' ALTER TABLE payments SET LOGGED ;''')
    cur.execute(f''' ALTER TABLE sales SET LOGGED ;''')
    db.commit()
    cur.execute('SET synchronous_commit TO on;')

def loadFolderCopy(path):
    directory=os.fsencode(path)
    files = (str(f).split("'")[1] for f in os.listdir(directory))
    for file in files:
        o=csv.DictReader(codecs.open(f'{path}\{file}','r',encoding='utf-8'))
        tableName,tableValues=getTableName(o)
        if tableName=='payments':
            cTableQuery=cur.execute(f'CREATE TABLE IF NOT EXISTS payments2 ({tableValues});')
        elif tableName=='sales':
            cTableQuery=cur.execute(f'CREATE TABLE IF NOT EXISTS sales2 ({tableValues});')
        
        fullPath=path2+"\\"+file
        copyQuery="COPY {}2 FROM '{}' csv header  ;".format(tableName,fullPath)
        cur.execute(copyQuery)
        print(f'Processed {file}')
    db.commit()

#loadFolderSlow(path)
#loadFolderCopy(path2)

def totalSalesPerDay(item):
    ''' Get list of daily total sales of item per whole date range  '''
    def findCorrectItem(item):
        cur.execute(""" SELECT DISTINCT description from sales2;""")
        itemsList=[v[0] for v in cur]
        try:
            item=difflib.get_close_matches(item,itemsList)[0]
            print=f'Searching for {item}'
            return item
        except IndexError:
            print(f'Didnt find {item}')
            sys.exit()

    def searchForTotals(item):
        cur.execute(f""" SELECT 
        sales2.description as name,
        to_date(payments2.paymenttime,'DD Mon YYYY') as date,
        sum(cast(sales2.quantity as int))
        FROM sales2 
        INNER JOIN payments2 ON payments2.checkitemid = sales2.checkitemid 
        WHERE sales2.description ='{item}'
        GROUP BY date,name
        ORDER BY date
        """)
        result=[v[2] for v in cur]
        if result:
            return result
        else:
            return False
    
    r = searchForTotals(item)
    if r is False:
        r = searchForTotals(findCorrectItem(item))
    return r

def EODSales():
    ''' End of day total sales '''
    cur.execute('''
    select 
    sum(cast(payments2.methodtotal as money)) as total,
    (to_timestamp(payments2.paymenttime,'DD Mon YYYY HH24:MI:SS') - interval '4 hours')::date as date
    from payments2
    WHEre payments2.paymentname not in ('DepositPayment')
    group by date
    ''')
    
    return [v for v in cur]

sales=totalSalesPerDay('50ml Monkey 47')
plt.plot(sales)
plt.show()
#eodSales=EODSales()

db.close()

pass