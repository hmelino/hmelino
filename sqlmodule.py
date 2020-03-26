import csv
import codecs
import os
import sqlite3
db=sqlite3.connect('db2.db')
cur=db.cursor()


def createSqlite3Database():
    path='E:\PythonCoding\Reports'
    directory=os.fsencode(path)
    files = [str(f).split("'")[1] for f in os.listdir(directory)]

    def createTableQuery(data):
        typeREAL=['Gratuity','MethodTotal','PaymentTotal','ServiceCharge']
        reportType={'PaymentName':'payments','Description':'sales'}
        reportIdentifier=list(data[0].keys())[4]
        dataType=['REAL' if k in typeREAL else 'TEXT' for k,v in data[0].items()]
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

    count=0
    for f in files:
        
        fullPath=f'{path}/{f}'
        o=csv.DictReader(codecs.open(fullPath,'r',encoding='utf-8'))
        fieldNames=o.fieldnames
        data = [v for v in o]
        createTable(data)
        for line in data:
            if fieldNames[4]=='PaymentName':
                cur.execute('INSERT INTO payments VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',list(line.values()))
            elif fieldNames[4]=='Description':
                cur.execute('INSERT INTO sales VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',list(line.values()))
        count+=1
        print(f'Completed {count}/{len(files)}')
            
createSqlite3Database()

db.commit()
db.close()
pass