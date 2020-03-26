import csv
import codecs
import os
import sqlite3
import datetime
db=sqlite3.connect('db7777.db')
cur=db.cursor()


def createSqlite3Database():
    path='E:\PythonCoding\Reports'
    directory=os.fsencode(path)
    files = [str(f).split("'")[1] for f in os.listdir(directory)]

    def createTableQuery(data):
        specialTypes={'Gratuity':'REAL','MethodTotal':'REAL','PaymentTotal':'REAL','ServiceCharge':'REAL','Paymenttime':'DATETIME','EventDate':'DATETIME'}
        reportType={'PaymentName':'payments','Description':'sales'}
        reportIdentifier=list(data[0].keys())[4]
        dataType=[specialTypes[k] if k in specialTypes else 'TEXT' for k,v in data[0].items()]
        # join fieldNames and dataTypes into tuple
        d=[(fieldNames[iN],dataType[iN]) for iN in range(len(dataType))]
        #join values into string
        values=''
        for v,k in d:
            d=f'{v} {k},'
            values+=d
        
        return f'CREATE TABLE {reportType[reportIdentifier]} ({values[:-1]})'

    def createTable(data):
        try:
            query=createTableQuery(data)
            cur.execute(query)
        except Exception:
            pass
            #Table already exists

    def removePound(lineData):
        lineData['ItemsTotal']=lineData['ItemsTotal'][1:]
        lineData['Net']=lineData['Net'][1:]
        lineData['TaxAmount']=lineData['TaxAmount'][1:]

        pass
    def convertDateV2(line):
        pyPayTime=datetime.datetime.strptime(line['Paymenttime'],'%d %b %Y %H:%M:%S')
        #pyEventTime=datetime.datetime.strptime(line['EventDate'],'%d %b %Y %H:%M:%S')
        line['Paymenttime']=pyPayTime.strftime('%Y-%m-%d %H:%M%S')
        #line['EventDate']=pyTime.strptime('%Y-%m-%d %H:%M%S')
    
    finishCount=0
    for f in files:
        
        fullPath=f'{path}/{f}'
        o=csv.DictReader(codecs.open(fullPath,'r',encoding='utf-8'))
        fieldNames=o.fieldnames
        data = [v for v in o]
        createTable(data)
        for line in data:
            lineData=line
            if fieldNames[4]=='PaymentName':
                convertDateV2(lineData)
                cur.execute('INSERT INTO payments VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',list(lineData.values()))
            elif fieldNames[4]=='Description':
                removePound(lineData)
                cur.execute('INSERT INTO sales VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',list(lineData.values()))
        finishCount+=1
        print(f'Completed {finishCount}/{len(files)}')
createSqlite3Database()

'2018-31-12 12:00:00'
'2007-01-01 10:00:00'

db.commit()
db.close()
pass