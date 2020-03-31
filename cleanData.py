def processReports():
    ''' clean data'''
    import os
    import csv
    import re
    def removeCommas(line):
        for key in line.keys():
            try:
                line[key]=re.sub(',','',line[key])
            except KeyError:
                pass
        return line

    path='E:\PythonCoding\Reports'
    path2='E:\PythonCoding\Reports2'
    f = os.fsencode(path)
    folder=[str(v)[2:-1] for v in os.listdir(f)]
    for filename in folder:
        inFile=f'{path}\{filename}'
        outFile=f'{path2}\{filename}'
        with open(inFile,'r') as file, open(outFile,'w',newline='') as outputFile:
            ddata=csv.DictReader(file)
            data=[removeCommas(v) for v in ddata]
            fNames=list(ddata.fieldnames)
            editedData=csv.DictWriter(outputFile,fNames)
            editedData.writerow({v:v for v in fNames})
            editedData.writerows(data)
            print(f'finished {filename}')

processReports()
pass