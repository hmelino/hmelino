import psycopg2
from psycopg2.extensions import AsIs
import os
import csv
import codecs
import time
import sys
import difflib
import matplotlib.pyplot as plt
import re


class Database:
    def __init__(self,dbname,user,host,password):
        self.db=psycopg2.connect(dbname=dbname,user=user,host=host, password=password )
        self.cur=self.db.cursor()
        self.db.autocommit=False

    def createTableValues(self,headers):
        d=tuple(f'{v} TEXT' for v in headers[1:])
        return d
        d=[f'{v} TEXT,' for v in headers]
        #remove last column in last list position
        d[len(d)-1]=d[len(d)-1][:-1]
        s=''.join(d)
        return s

    def getReportName(self,readerObject):
        headers=readerObject.fieldnames
        tableValues=self.createTableValues(headers)
        if headers[4]=='PaymentName':
            return 'payments',tableValues
        elif headers[4]=='Description':
            return 'sales',tableValues
        else:
            print('unknow file')
            sys.exit()

    def loadFolderInsert(self,path):
        cur=self.cur
        ''' Fill up database with INSERT query'''
        cur.execute('SET synchronous_commit TO off;') 
        directory=os.fsencode(path)
        files = (str(f).split("'")[1] for f in os.listdir(directory))
        for file in files:
            o=csv.DictReader(codecs.open(f'{path}\{file}','r',encoding='utf-8'))
            tableName,tableValues=self.getReportName(o)
            if tableName=='payments':
                cur.execute('CREATE TABLE IF NOT EXISTS payments (Sitename TEXT,BillheaderID TEXT,CheckitemID TEXT,PaymentName TEXT,CustomerTable TEXT,LocationName TEXT,TerminalName TEXT,Paymenttime TEXT,PaymentTakenByName TEXT,PaymentGroupName TEXT,Info TEXT,Details TEXT,ServiceCharge TEXT,MethodTotal TEXT,Gratuity TEXT,PaymentTotal TEXT,Covers TEXT,DepositOwner TEXT,EventDate TEXT,DepositID TEXT,SplitNumber TEXT);')
            #len21
            elif tableName=='sales':
                cur.execute('CREATE TABLE IF NOT EXISTS sales (LocationName TEXT,RevenueCentreName TEXT,ClassName TEXT,Description TEXT,CheckItemID TEXT,UserName TEXT,Quantity TEXT,ItemsTotal TEXT,Net TEXT,TaxAmount TEXT,Textbox75 TEXT,Textbox76 TEXT,Textbox77 TEXT,Textbox78 TEXT,Textbox66 TEXT,Textbox67 TEXT,Textbox68 TEXT,Textbox69 TEXT,Textbox57 TEXT,Textbox58 TEXT,Textbox59 TEXT,Textbox46 TEXT,Textbox47 TEXT,Textbox48 TEXT,Textbox114 TEXT,Textbox115 TEXT,Textbox116 TEXT,Textbox34 TEXT,Textbox35 TEXT,Textbox36 TEXT)')
            #len30
            cur.execute(f'ALTER TABLE {tableName} SET UNLOGGED;')
            dataLeft=True
            while dataLeft:
                try:
                    values=list(next(o).values())
                    query=f'INSERT INTO {tableName} VALUES (%s)'
                    cur.execute(query,(values,))
                except StopIteration:
                    dataLeft=False
            print(f'Loaded {file}')
        cur.execute(f''' ALTER TABLE payments SET LOGGED ;''')
        cur.execute(f''' ALTER TABLE sales SET LOGGED ;''')
        self.db.commit()
        cur.execute('SET synchronous_commit TO on;')

    def cleanData(self,dirtyReports,cleanReports):
        """dirtyReports = 'path to folder of reports to be processed'
        cleanReports = 'path to empty folder for clean processed , if you use existin folder
        ALL DATA INSIDE WILL BE DELETED  """
        def removeCommas(line):
            for key in line.keys():
                try:
                    line[key]=re.sub(',','',line[key])
                except KeyError:
                    pass
            return line

        f = os.fsencode(dirtyReports)
        folder=[str(v)[2:-1] for v in os.listdir(f)]

        if not os.path.isdir(cleanReports):
            print(f'Creating new folder {cleanReports}')
            os.mkdir(cleanReports)
        for filename in folder:
            inFile=f'{dirtyReports}\{filename}'
            outFile=f'{cleanReports}\{filename}'
            
            with open(inFile,'r') as file, open(outFile,'w',newline='') as outputFile:
            
                
                ddata=csv.DictReader(file)
                data=[removeCommas(v) for v in ddata]
                fNames=list(ddata.fieldnames)
                editedData=csv.DictWriter(outputFile,fNames)
                editedData.writerow({v:v for v in fNames})
                editedData.writerows(data)
                print(f'finished {filename}')

    def loadFolderCopy(self,folderPath):
        cur=self.cur
        directory=os.fsencode(folderPath)
        files = (str(f).split("'")[1] for f in os.listdir(directory))
        for file in files:
            o=csv.DictReader(codecs.open(f'{folderPath}\{file}','r',encoding='utf-8'))
            tableName,tableValues=self.getReportName(o)
            fullPath=folderPath+"\\"+file
            if tableName=='payments':
                cur.execute('CREATE TABLE IF NOT EXISTS payments (Brand TEXT,Sitename TEXT,BillheaderID TEXT,CheckitemID TEXT,PaymentName TEXT,CustomerTable TEXT,LocationName TEXT,TerminalName TEXT,Paymenttime TEXT,PaymentTakenByName TEXT,PaymentGroupName TEXT,Info TEXT,Details TEXT,ServiceCharge TEXT,MethodTotal TEXT,Gratuity TEXT,PaymentTotal TEXT,Covers TEXT,DepositOwner TEXT,EventDate TEXT,DepositID TEXT,SplitNumber TEXT,ReceiptNumber TEXT);') 
                #23 values
                copyQuery="COPY payments FROM '{}' csv header  ;".format(fullPath)
            elif tableName=='sales':
                cTableQuery=cur.execute(f'CREATE TABLE IF NOT EXISTS sales (Sitename TEXT,LocationName TEXT,RevenueCentreName TEXT,ClassName TEXT,Description TEXT,CheckItemID TEXT,UserName TEXT,Quantity TEXT,ItemsTotal TEXT,Net TEXT,TaxAmount TEXT,Textbox75 TEXT,Textbox76 TEXT,Textbox77 TEXT,Textbox78 TEXT,Textbox66 TEXT,Textbox67 TEXT,Textbox68 TEXT,Textbox69 TEXT,Textbox57 TEXT,Textbox58 TEXT,Textbox59 TEXT,Textbox46 TEXT,Textbox47 TEXT,Textbox48 TEXT,Textbox114 TEXT,Textbox115 TEXT,Textbox116 TEXT,Textbox34 TEXT,Textbox35 TEXT,Textbox36 TEXT);')
                #31 values
                copyQuery="COPY sales FROM '{}' csv header  ;".format(fullPath)
            cur.execute(copyQuery)
            
            cur.execute(copyQuery)
            print(f'Processed {file}')
        self.db.commit()

    def totalSalesPerDay(self,item):
        cur=self.cur
        ''' Get list of daily total sales of item per whole date range  '''
        def findCorrectItem(item):
            cur.execute(""" SELECT DISTINCT description from sales;""")
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
            sales.description as name,
            to_date(payments.paymenttime,'DD Mon YYYY') as date,
            sum(cast(sales.quantity as int))
            FROM sales 
            INNER JOIN payments ON payments.checkitemid = sales.checkitemid 
            WHERE sales.description ='{item}'
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

    def EODSales(self):
        cur=self.cur
        ''' End of day total sales '''
        cur.execute('''
        select 
        sum(cast(payments.methodtotal as money)) as total,
        (to_timestamp(payments.paymenttime,'DD Mon YYYY HH24:MI:SS') - interval '4 hours')::date as date
        from payments
        WHEre payments.paymentname not in ('DepositPayment')
        group by date
        ''')
        
        return [v for v in cur]

pass