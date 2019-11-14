# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 18:10:02 2019

@author: babuj
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 12:04:56 2019

@author: Babuj
"""

import sys
import time

def filelist(noofdays):
    filelist =[]
    current = 20191107
    #current = int(time.strftime("%Y%m%d"))
    for i in range(1,noofdays):
        current = str(current-1)
        filename = "mod_conf_T1_"+current+".csv"
        filelist.append(filename)
        current =int(current)    
    return filelist

def listof_filereading(dir_path,filelist):
    rawdata =[]
    for i in filelist:
        sep =r"\\"
        path = dir_path + sep + i
        if i.endswith('.csv'):
            with open(path, 'r') as f:
    #read from csv line by line, rstrip helps to remove '\n' at the end of line
                lines = [line.rstrip() for line in f] 
                rawdata.append(lines)
    return rawdata

def filereading(path):
    rawdata =[]
    with open(path, 'r') as f:
    #read from csv line by line, rstrip helps to remove '\n' at the end of line
        lines = [line.rstrip() for line in f] 
        rawdata.append(lines)
    return rawdata

def DataParsing(rawdata,slave_id):
    #"triggerDate","disTemperature2","disPressure2","sumpPressure2","generalStatus","faultStatus","vfdCurrentSpeedPercent2"
    results = []
    data_file=[]
    for x in rawdata:
        for line in x:
            words = line.split('|')
            #get each item in one line
            first = words[0].split(',')
            date = first[0] 
            words[0]= words[0].replace(date+',','')
            result =[]
            for i in words: 
                split_data = i.split(',')
                split_data.insert(0,date)
                result.append(split_data)
            results.append(result)
    for y in results:
        for t in y:
            if(t[1]==slave_id):
                data_file.append(t)
    return data_file 

def DataCleaning(data_file):
    data_file1 =[]
#    for y in data_file:
#        print(len(y))
    for i in data_file:
        data1 = []
        datetime = i[0]
        d = datetime.split(' ')
        date = d[0].split('-')
        tim = d[1].split(':')
        dttuple = (int(date[0]),int(date[1]),int(date[2]),int(tim[0]),int(tim[1]),int(tim[2]),0,0,0)
        i[0] = int(time.mktime(dttuple))
        data1.append(i[0])
        a = len(i)
        for x in range(1,a):
            data1.append(int(i[x]))   
        data_file1.append(data1)   
    return data_file1
        
def DataParsingwithload(Data,slave_id,load_slaveid,load_pos):
    #"triggerDate","disTemperature2","disPressure2","sumpPressure2","generalStatus","faultStatus","vfdCurrentSpeedPercent2"
    results = []
    data_file=[]
    for x in Data:
        for line in x:
            words = line.split('|')
            #get each item in one line
            first = words[0].split(',')
            date = first[0] 
            words[0]= words[0].replace(date+',','')
            result =[]
            for i in words: 
                split_data = i.split(',')
                split_data.insert(0,date)
                result.append(split_data)
            results.append(result)
    for y in results:
        for t in y:
           
            if(t[1]==load_slaveid):
                temp = t[load_pos]
                
            if(t[1]==slave_id):
                t.insert(len(t),temp)
                data_file.append(t) 
                
    return data_file  

def average(data1,pos,days):
#    print("---- Average function ------")
    sum=0
    current = r'7/11/2019'
#    current = time.strftime("%d/%m/%Y")
    date1 = current.split('/')
    tuple2 =(int(date1[2]),int(date1[1]),int(date1[0]),0,0,0,0,0,0)
    epoch_current = int(time.mktime(tuple2))
    start = epoch_current - (days*24*60*60)
    count=0
    for x in data1:
        if (x[0] > start ):
            if (x[0] < epoch_current):
                sum = sum+x[pos]
                count =count+1
    if(count == 0):
        return(0)
    return (sum/count)    

def averagewithload(data1,pos,days):
#    print("---- Average function ------")
    sum=0
    current = r'7/11/2019'
#    current = time.strftime("%d/%m/%Y")
    date1 = current.split('/')
    tuple2 =(int(date1[2]),int(date1[1]),int(date1[0]),0,0,0,0,0,0)
    epoch_current = int(time.mktime(tuple2))
    start = epoch_current - (days*24*60*60)
    count=0
    for x in data1:
        if (x[0] > start ):
            if (x[0] < epoch_current):
                if(x[len(x)-1] == 8): # during load condition
                    sum = sum+x[pos]
                    count =count+1
    if(count == 0):
        return(0)
    return (sum/count)

def rulecheck(value,threshold):
#    print("---- rulecheck function ------")
    if(value > threshold):
        print("DOT Rule")
        print("True")
        print("AVG Dis.Temp :" + str(value))
    else:
        print("DOT Rule")
        print("False")
        print("AVG Dis.Temp :" + str(value))

def comparerulecheck(value1,value2,thrshold):
    #print("---- comparerulecheck function ------")
    compare = value2 #setvalue
    ulti = (compare/value1)*100
    if(ulti > thrshold):
        print("Compressor running lower than set load pressure")
        print("True")
        print("AVG Dis.Pressure :"+ str(value1))
        print("AVG SetLoad.Pressure :"+ str(value2))
        print("Utilization :"+ str(ulti))
    else:
        print("Compressor running lower than set load pressure")
        print("False")
        print("AVG Dis.Pressure :"+ str(value1))
        print("AVG SetLoad.Pressure :"+ str(value2))
        print("Utilization :"+ str(ulti))
        
def comparerulecheck2(avg_dis_pr,avg_set_pr,avg_vfd_speed,thrshold):
    #print("---- comparerulecheck2 function ------") 
    ulti = (avg_set_pr/avg_dis_pr)*100
    if(avg_vfd_speed > thrshold):
        if(ulti > 100):
            print("VFD - Compressor running lower than set load pressure")
            print("True")
            print("AVG Dis.Pressure :"+ str(avg_dis_pr))
            print("AVG SetLoad.Pressure :"+ str(avg_set_pr))
            print("AVG VFD_Speed:"+str(avg_vfd_speed))
            print("Utilization :"+ str(ulti))
        else:
            print("VFD - Compressor running lower than set load pressure")
            print("False")
            print("AVG Dis.Pressure :"+ str(avg_dis_pr))
            print("AVG SetLoad.Pressure :"+ str(avg_set_pr))
            print("AVG VFD_Speed:"+str(avg_vfd_speed))
            print("Utilization :"+ str(ulti))
    else:
        print("VFD - Compressor running lower than set load pressure")
        print("False")
        print("AVG Dis.Pressure :"+ str(avg_dis_pr))
        print("AVG SetLoad.Pressure :"+ str(avg_set_pr))
        print("AVG VFD_Speed:"+str(avg_vfd_speed))
        print("Utilization :"+ str(ulti))

def eql_duration(fulldata,pos,threshold,duration):
    #current = time.strftime("%d/%m/%Y")
    #print("---- duration function ------")
    current = r'7/11/2019'
    date1 = current.split('/')
    tuple2 =(int(date1[2]),int(date1[1]),int(date1[0]),0,0,0,0,0,0)
    epoch_current = int(time.mktime(tuple2))
    start = epoch_current - (1*24*60*60)
    dur = (duration/5)-1
    count=0
    isrule = 0
    previous = start
    for x in fulldata:
        if (x[0] > start and x[0] < epoch_current):
            diff = x[0]-previous
            if(diff < 9):
                if(x[pos] == threshold):
                    count = count+1
                    if(count > dur and isrule == 0):
                        isrule = 1
                        print(x[0])
                else:
                    count = 0
                    isrule = 0
            else:
                count = 0
                isrule = 0
        previous = x[0]

def gt_duration(fulldata,pos,threshold,duration):
    #current = time.strftime("%d/%m/%Y")
    #print("---- duration function ------")
    current = r'7/11/2019'
    date1 = current.split('/')
    tuple2 =(int(date1[2]),int(date1[1]),int(date1[0]),0,0,0,0,0,0)
    epoch_current = int(time.mktime(tuple2))
    start = epoch_current - (1*24*60*60)
    dur = (duration/5)-1
    count=0
    isrule = 0
    previous = start
    for x in fulldata:
        if (x[0] > start and x[0] < epoch_current):
            diff = x[0]-previous
            if(diff < 9):
                if(x[pos] > threshold):
                    count = count+1
                    if(count > dur and isrule == 0):
                        isrule = 1
                        print(x[0])
                else:
                    count = 0
                    isrule = 0
            else:
                count = 0
                isrule = 0
        previous = x[0]

def lt_duration(fulldata,pos,threshold,duration):
    #current = time.strftime("%d/%m/%Y")
    #print("---- duration function ------")
    timelist =[]
    current = r'7/11/2019'
    date1 = current.split('/')
    tuple2 =(int(date1[2]),int(date1[1]),int(date1[0]),0,0,0,0,0,0)
    epoch_current = int(time.mktime(tuple2))
    start = epoch_current - (1*24*60*60)
    dur = (duration/5)-1
    count=0
    isrule = 0
    previous = start
    for x in fulldata:
        if (x[0] > start and x[0] < epoch_current):
            diff = x[0]-previous
            if(diff < 9):
                if((x[len(x)-1] == 8) and (x[pos] < threshold)):
                    count = count+1
                    if(count > dur and isrule == 0):
                        isrule = 1
                        time1 = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x[0]))
                        timelist.append(time1)
                else:
                    count = 0
                    isrule = 0
            else:
                count = 0
                isrule = 0
        previous = x[0]
    return timelist

def durationcheck(timelist):
    length = len(timelist)
    if( length == 0):
        print("Sump Pressure is not developing")
        print("False")
    else:
        print("Sump Pressure is not developing")
        print("True")
        print("No of occurrence:"+ str(length))
        for x in timelist:
            print(x)

def count(data1,pos,faultstatus):
    #current = time.strftime("%d/%m/%Y")
    #print("---- count function ------")
    current = r'7/11/2019'
    date1 = current.split('/')
    tuple2 =(int(date1[2]),int(date1[1]),int(date1[0]),0,0,0,0,0,0)
    epoch_current = int(time.mktime(tuple2))
    start = epoch_current - (1*24*60*60)
    count=0
    isrule = 0
    for x in data1:
        if (x[0] > start and x[0] < epoch_current):
            if(x[pos] == faultstatus):
                if(isrule == 0):
                    count = count + 1
                    isrule = 1
            else:
                isrule = 0
    return count
       
def countcheck(count,threshold,check):
    if(check == 0):
        count = count
        if(count > threshold):
            print("Neuron trip")
            print("True")
            print("No of occurrence:"+str(count))
        else:
            print("Neuron trip")
            print("False")
            print("No of occurrence:"+str(count))
    else:
        count = count-1
        if(count > threshold):
            print("More no of load/unload count")
            print("True")
            print("No of occurrence:"+str(count))
        else:
            print("More no of load/unload count")
            print("False")
            print("No of occurrence:"+str(count))
    
    
def main():
    #path = r'C:\Users\babuj\Documents\Data'
    PATH = r'C:\Users\babuj\Desktop\Data\test'
#    files = filelist(int(sys.argv[2])+1)
#    rawdata = listof_filereading(PATH,files)
#   data = filereading(path)
    if(len(sys.argv) < 4):
        print("require atleast 3 parameter")
    else:
        if(sys.argv[1] == '1'):#DOT rule #values(1 1 1 5 105 1 6)
            files = filelist(int(sys.argv[2])+1)
            rawdata = listof_filereading(PATH,files)
            datafile = DataParsingwithload(rawdata,sys.argv[3],sys.argv[6],int(sys.argv[7]))
            temp_data = DataCleaning(datafile)
            avg_temp = averagewithload(temp_data,int(sys.argv[4]),int(sys.argv[2]))
            rulecheck(avg_temp,int(sys.argv[5]))
        elif(sys.argv[1] == '2'): #Dis.pr > Set.Pr #values(2 1 2 4 2 5 100 1 6)
            files = filelist(int(sys.argv[2])+1)
            rawdata = listof_filereading(PATH,files)
            datafile = DataParsingwithload(rawdata,sys.argv[3],sys.argv[8],int(sys.argv[9]))
            dis_pr_data = DataCleaning(datafile)
            datafile1 = DataParsingwithload(rawdata,sys.argv[5],sys.argv[8],int(sys.argv[9]))
            set_pr_data = DataCleaning(datafile1)
            avg_dis_pr = averagewithload(dis_pr_data,int(sys.argv[4]),int(sys.argv[2]))
            avg_set_pr = averagewithload(set_pr_data,int(sys.argv[6]),int(sys.argv[2]))
            comparerulecheck(avg_dis_pr,avg_set_pr,int(sys.argv[7]))
        elif(sys.argv[1] == '3'): #vfd values(3 1 2 4 2 5 2 6 95 1 6)
            files = filelist(int(sys.argv[2])+1)
            rawdata = listof_filereading(PATH,files)
            datafile = DataParsingwithload(rawdata,sys.argv[3],sys.argv[10],int(sys.argv[11]))
            dis_pr_data = DataCleaning(datafile)
            datafile1 = DataParsingwithload(rawdata,sys.argv[5],sys.argv[10],int(sys.argv[11]))
            set_pr_data = DataCleaning(datafile1)
            datafile2 = DataParsingwithload(rawdata,sys.argv[7],sys.argv[10],int(sys.argv[11]))
            vfd_data = DataCleaning(datafile2)
            avg_dis_pr = averagewithload(dis_pr_data,int(sys.argv[4]),int(sys.argv[2]))
            avg_set_pr = averagewithload(set_pr_data,int(sys.argv[6]),int(sys.argv[2]))
            avg_vfd_speed = averagewithload(vfd_data,int(sys.argv[8]),int(sys.argv[2]))
            comparerulecheck2(avg_dis_pr,avg_set_pr,avg_vfd_speed,int(sys.argv[7]))
        elif(sys.argv[1] == '4'): #sum_pr <3.5 values(4 3 4 35 30 1 6) (oneday only)
            files = filelist(int(1)+1)
            rawdata = listof_filereading(PATH,files)
            datafile = DataParsingwithload(rawdata,sys.argv[2],sys.argv[6],int(sys.argv[7]))
            sum_pr_data = DataCleaning(datafile)
            timelist = lt_duration(sum_pr_data,int(sys.argv[3]),int(sys.argv[4]),int(sys.argv[5]))
            durationcheck(timelist)
        elif(sys.argv[1] == '5'): #loadunload values(5 1 6 8 10)
            files = filelist(int(1)+1)
            rawdata = listof_filereading(PATH,files)
            datafile = DataParsing(rawdata,sys.argv[2])  
            general_status_data = DataCleaning(datafile)
            n = count(general_status_data,int(sys.argv[3]),int(sys.argv[4]))
            countcheck(n,int(sys.argv[5]),1)
        elif(sys.argv[1] == '6'): #neuron_trip values(6 1 6 8 10)
            files = filelist(int(1)+1)
            rawdata = listof_filereading(PATH,files)
            datafile = DataParsing(rawdata,sys.argv[2])  
            fault_status_data = DataCleaning(datafile)
            n = count(fault_status_data,int(sys.argv[3]),int(sys.argv[4]))
            countcheck(n,int(sys.argv[5]),0)

if __name__ == "__main__":
    main()
