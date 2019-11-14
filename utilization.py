# -*- coding: utf-8 -*-
"""
Created on Mon Sep 23 10:50:02 2019

@author: Babuj
"""

from sqlalchemy import create_engine
import pandas as pd
import datetime
import threading 
import os

location = r'C:\Users\babuj\Desktop\result2.xlsx'
df = pd.read_excel(location) 

fabnolist=df.values[:,0].tolist()

fabnolist1 = fabnolist[0:21]
fabnolist2 = fabnolist[22:43]
fabnolist3 = fabnolist[44:65]
fabnolist4 = fabnolist[66:87]
fabnolist5 = fabnolist[88:109]
fabnolist6 = fabnolist[110:131]
fabnolist7 = fabnolist[132:143]
fabnolist8 = fabnolist[144:166]

password = r'ElgiHost@1960'
connection_string = 'mysql+pymysql://root:'+password+'@103.249.97.147/ecms'

db_connection = create_engine(connection_string)

def execution(fabnol,file,k):
    j=k
    for x in fabnol:
        print(j)
        start = datetime.datetime(2019,11,10,00,00,00)
        enddate = datetime.datetime(2019,11,14,00,00,00)
        end = start + datetime.timedelta(days=2)
        while end<=enddate:
            df_sql =pd.read_sql("select fabno,triggerDate,count(fabNo),avg(disPressure2) as avgdispre from trendsub2 where fabNo=\'"+str(x)+"\' and triggerDate > \'"+str(start)+"\' and triggerDate < \'"+str(end)+"\' and generalStatus=8", con=db_connection)
            df_set =pd.read_sql("select fabno,setLoadPressure from trendsub1 where fabNo=\'"+str(x)+"\' and triggerDate > \'"+str(start)+"\' limit 1",con=db_connection)
            df_sql = df_sql.dropna()
            df_set = df_set.dropna()
            result = df_sql.empty
            result1 = df_set.empty
            if (not result) and (not result1):
                a = float(df_sql['avgdispre'])
                b = float(df_set['setLoadPressure'])
                if (a < b):
                    result = pd.merge(df_sql, df_set, on='fabno')
                    result['percentage'] = result.setLoadPressure*100/result.avgdispre
#                    print result
                    result.to_csv(file, mode='a', header=True)
#                   print df_set        
            start = start + datetime.timedelta(days=1)
            end = start + datetime.timedelta(days=1)
        j=j+1

if __name__ == "__main__": 
    output_location=r'C:\Users\babuj\Desktop\Ruleoutput\utilization.csv'
    if(os.path.exists(output_location)):
        os.remove(output_location)
    pgm_start = datetime.datetime.now()
    print(pgm_start)
    t1 = threading.Thread(target=execution, args=(fabnolist1,'faultstatus1.csv',0)) 
    t2 = threading.Thread(target=execution, args=(fabnolist2,'faultstatus2.csv',22)) 
    t3 = threading.Thread(target=execution, args=(fabnolist3,'faultstatus3.csv',44))
    t4 = threading.Thread(target=execution, args=(fabnolist4,'faultstatus4.csv',66))
    t5 = threading.Thread(target=execution, args=(fabnolist5,'faultstatus5.csv',88))
    t6 = threading.Thread(target=execution, args=(fabnolist6,'faultstatus6.csv',110))
    t7 = threading.Thread(target=execution, args=(fabnolist7,'faultstatus7.csv',132))
    t8 = threading.Thread(target=execution, args=(fabnolist8,'faultstatus8.csv',144))
  
    # starting thread 1 
    t1.start() 
    # starting thread 2 
    t2.start() 
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()
  
    # wait until thread 1 is completely executed 
    t1.join() 
    # wait until thread 2 is completely executed 
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    t8.join()
  
    # both threads completely executed 
    print("Done!")
    frames =[]
    filenames = ['faultstatus1.csv','faultstatus2.csv','faultstatus3.csv','faultstatus4.csv','faultstatus5.csv','faultstatus6.csv','faultstatus7.csv','faultstatus8.csv']
    for f in filenames:
        if(os.path.exists(f)):
            frames.append(f)
#            result = pd.read_csv(f)
#            combained_csv=pd.concat(result)
#            print (combined_csv)
#    for f in frames:
#        combined_csv = pd.concat(pd.read_csv(f))
#    print(frames)
    combined_csv = pd.concat( [ pd.read_csv(f) for f in frames ] )
    duplicated = combined_csv.drop_duplicates()
#    print(combined_csv)
    duplicated.to_csv( output_location, index=False )
    for f in frames:
        os.remove(f)
    fault = pd.read_csv(output_location)
    lsit= fault['fabno'].unique().tolist()
    print(lsit)
    pgm_end = datetime.datetime.now()
    print(pgm_end)
    print("Total time passed is {} :".format(pgm_end-pgm_start))
