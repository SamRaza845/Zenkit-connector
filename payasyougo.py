import psycopg2
import socket
from datetime import timedelta,datetime,timezone
import threading
import pandas as pd
import time
from psycopg2 import Error
from getmac import get_mac_address
import os , platform
import tkinter, tkinter.messagebox
import ast
import csv

def messagebox():
    errmsg = "An Important File is Missing Kindly Contact: admin@lyftrondata.com"
    root = tkinter.Tk()
    root.withdraw()
    tkinter.messagebox.showerror("Critical Error", errmsg)
    root.destroy()
    os._exit(0)
    
threadLock = threading.Lock()
TimeStringFormat = '%H:%M:%S.%f'

class DB_THREAD (threading.Thread):
    ''' DB Thread purpose is to make each Method logs entires in entirely seperate thread
    to boost the performance of our model.
    
    @func: __init__
    @param LicenseKey: LicenseKey Provided by the LyftronData
    @type LicenseKey: str
    @param  Start_Time: execution time of the function
    @type   Start_Time: datetime
    @param  End_time: time at which execution ended
    @type   End_time: datetime
    @param  TotalTime: Total time Taken by the function to execute and end
    @type   TotalTime: str
    @param  message
    @type   message: str
    @param  FunctionName
    @type   FunctionName: str
    '''
    def __init__(self,Start_Time,End_Time,TotalTime,message,LicenseKey,FunctionName,connectername):
        threading.Thread.__init__(self)
        self.name = FunctionName
        self.LicenseKey = LicenseKey
        self.connectername = connectername
        self.starttime = Start_Time
        self.endtime = End_Time
        self.totaltime = TotalTime 
        self.message = message
    def run(self):
        threadLock.acquire()
        #readSchedule(self.connectername)
        insert(self.LicenseKey,self.name,self.starttime,self.endtime,self.totaltime,self.message)
        threadLock.release()
        
def measure_execution_time(LicenseKey,FunctionName,StartTime,EndTime,connectername):
    '''A Method used to Measure the Start Time and End Time of the execution of given Function

        @param LicenseKey: The Key provided by Lyftron Data to user
        @type  LicenseKey: str 
        @param FunctionName: Function Name whom Execution time to be Calculated
        @type  FunctionName: str
        @param  StartTime: Starting Time Taken at the time of method execution
        @type StartTime: time
        @type EndTime: time
        @param EndTime: End Time Taken after the Complete exectuion of method              
    '''    
    Start_Time = datetime.utcfromtimestamp(StartTime)
    End_Time = datetime.utcfromtimestamp(EndTime)
    StartTimeStamp = "{PATTERN}\n{FUNCTION_NAME}\t Start time: {START_TIME} \t"\
            .format(PATTERN=50*'-',FUNCTION_NAME=FunctionName,START_TIME=str(Start_Time))
    EndTimeStamp = "\n{FUNCTION_NAME}\tEnd time: {END_TIME}"\
            .format(FUNCTION_NAME=FunctionName,END_TIME=str(End_Time))

    TotalTime=datetime.utcfromtimestamp(EndTime-StartTime).strftime(TimeStringFormat)

    TotalTimeStamp = "\n{FUNCTION_NAME}\tElapsed time: {ELAPSED_TIME} \n"\
            .format(FUNCTION_NAME=FunctionName,ELAPSED_TIME=TotalTime)
    timestamps = open('TimeStamps.txt','a')
    timestamps.write(StartTimeStamp)
    timestamps.write(EndTimeStamp)
    timestamps.write(TotalTimeStamp)
    timestamps.close()
    message = ("FUNCTION ({FUNCTION_NAME}) START AT ({START_TIME}) END AT ({END_TIME}) Total Time Taken ({TOTAL_TIME})").format(FUNCTION_NAME=FunctionName,START_TIME=Start_Time,END_TIME=End_Time,TOTAL_TIME = TotalTime)
    Thread = DB_THREAD(Start_Time,End_Time,TotalTime,message,LicenseKey,FunctionName,connectername)
    Thread.start()

def openConnection():
    '''Create the connection to the database 
    
        @rtype: tuple 
        @returns: A tuple containing connection to database and cursor
    '''
    connection = psycopg2.connect(database="ntaqwfpq", user = "ntaqwfpq", password = "qA9_GBtA4wqd2ocIjl013qeVIGOs7oD7", host = "satao.db.elephantsql.com", port = "5432")
    cursor = connection.cursor()
    return cursor,connection

def closeConnection(connection):
    
    '''Close the Active Connection to the Database

        @param Connection: tuple containing connection and cursor of active connection
        @type Connection: tuple
        @rtype: tuple
        @returns: A tuple containing Closed Connection and cursor 
    '''
    return connection[0].close(),connection[1].close()

def executeQuery(connection,table_name,query,val):
    '''This Method used to execute the queries for the active database connection

        @param connection: tuple containing the connection and cursor of active database
        @type connection: tuple 
        @param query: query to be executed 
        @type query: list
        @param val : Values in the query
        @type val: list
        @param table_name: Name of table whose queries and values are taken
        @type table_name: list
        @rtype connection.cursor
        @return: returns cursor if the given connection
    '''
    try:
        connection[0].execute(query,val)
        connection[1].commit()
        return connection[0]
    except Exception as e:
        print (getattr(e, 'message\n', repr(e)))
        writeSchedule(table_name,val)
        connection[1].commit()
        return False
        
def searchLicense(LicenseKey):
    ''''This Method Search the LicenseKey provided by Lyftrondata

        @param LicenseKey: License Key Provided by Lyftrondata
        @type LicenseKey: str
        @rtype : Boolean
        @return:  True if Key Exists in Lyftron DataBase else provide False
    '''
    connection = openConnection()
    query = "SELECT * FROM Registration_Form WHERE LicenseKey = %s;"
    val = (LicenseKey,) 
    cursor = executeQuery(connection,"Reistration_Form",query,val)
    result = cursor.fetchall()
    if not result:
        return False
    else:
        timeleft = "select remaining_time from payasyougo where id = (SELECT max(id) FROM PAYASYOUGO WHERE LicenseKey=%s);"
        remain = executeQuery(connection,"PAYASYOUGO",timeleft,(LicenseKey,))
        timeleft = remain.fetchone()
        timeleft = string_to_timedelta(timeleft[0])
        remainingtime = timeleft.total_seconds()
        if remainingtime<=0:
            print("Your Key is Expired")
            return False
        else:
            return True

def string_to_timedelta(string):
    '''This method is used for conversion of data type from String to timedelta

        @param string: String in time format e.g.( 2 days , 23:44:03.4444 ) -----> ( day, %H:%M:%S.f)
        @type string : str
        @rtype: timedelta
        @returns: time in timedelta format
    '''
    t1 = string.split()
    if len(t1)<3:
        #This Statement is used for the condition where the Time is Provided in (%H:%M:%S.f) Format
        ts = datetime.strptime(str(t1[0]),TimeStringFormat)
        td = timedelta(hours=ts.hour,minutes=ts.minute,seconds=ts.second,microseconds=ts.microsecond)
        return td
    else:
        #This Statement is used for the condition where the Time is Provided in (day , %H:%M:%S.f) Format
        day = float(t1[0])
        time_format = str(t1[-1])
        td1 = timedelta(days=day)
        ts = datetime.strptime(time_format,TimeStringFormat)
        td2 = timedelta(hours=ts.hour,minutes=ts.minute,seconds=ts.second,microseconds=ts.microsecond)
        return td1+td2

def timedelta_to_string(time_delta):
    time_delta_string = str(time_delta)
    t = time_delta_string.split()
    if len(t) <3:
        return time_delta_string
    else:    
        day=t[1].split(",")
        time_string = ("{days} {day} {Hours}").format(days=t[0],day=day[0],Hours=t[-1])
        return time_string

def insert(LicenseKey,FUNCTION,START_TIME,END_TIME,TOTAL_TIME_TAKEN,MESSAGE):
    '''This Method insert all the relative data into Lyftron Database
    
        @param LicenseKey: LicesneKey Provided by LyftronData.com
        @type LicenseKey: str
        @param FUNCTION: Executed Function Name
        @type FUNCTION: str
        @param START_TIME : Start time of function Execution
        @type START_TIME: datetime
        @param END_TIME: End time of function Execution
        @type END_TIME: datetime
        @param TOTAL_TIME_TAKEN: Total Time Taken by Function to Execute
        @type TOTAL_TIME_TAKEN: str
        @param MESSAGE: contain the complete information regarding Execution
        @type MESSAGE: str
    '''
    
    connection = openConnection()
    MAC = get_mac_address()
    Machine_Name = socket.gethostname()
    MONTH = str(datetime.now().strftime("%B-%Y"))
    IP = socket.gethostbyname(Machine_Name)
    
    #selecting max id from payasyougo last entry provided by the key
    select_maxid_payasyougo_query = "select * from payasyougo where id = (SELECT max(id) FROM PAYASYOUGO WHERE LicenseKey=%s);"
    max_idcursor = executeQuery(connection,"PAYASYOUGO",select_maxid_payasyougo_query,(LicenseKey,))
    rows = max_idcursor.fetchall() 
    try:
        for i in rows:
            TIME_SPENT = string_to_timedelta(TOTAL_TIME_TAKEN)+string_to_timedelta(i[4])
            RT = string_to_timedelta(i[3]) - TIME_SPENT
            ROWS_ALLOWED = 200
            ROWS_FETCHED = 0

        insert_in_payasyougo_query = "INSERT INTO PAYASYOUGO(LicenseKey,TOTAL_ALLOWED_TIME,REMAINING_TIME,TOTAL_TIME_SPENT,START_TIME,END_TIME,TOTAL_TIME_TAKEN,ROWS_ALLOWED,ROWS_FETCHED,MONTH,Machine_Name,MAC,IP) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        insert_in_payasyougo_val = (LicenseKey,i[3] ,timedelta_to_string(RT),timedelta_to_string(TIME_SPENT),str(START_TIME),str(END_TIME),TOTAL_TIME_TAKEN,str(ROWS_ALLOWED),str(ROWS_FETCHED),MONTH,Machine_Name,MAC,IP,)
        insert_in_Message_query = "INSERT INTO MESSAGE(LicenseKey,Machine_Name,MAC,MESSAGE) VALUES (%s,%s,%s,%s);"
        insert_in_Message_val = (LicenseKey,Machine_Name,MAC,MESSAGE,)

        select_bill_total_cost_query = "SELECT total_cost FROM BILL WHERE id =(SELECT max(id) FROM BILL where LicenseKey = %s);"
        bill_cursor = executeQuery(connection,"BILL",select_bill_total_cost_query,(LicenseKey,))
        bill = bill_cursor.fetchone()

        t1 = string_to_timedelta(TOTAL_TIME_TAKEN)
        insert_in_Bill_query = "INSERT INTO BILL(LicenseKey,Machine_Name,MAC,Time_spent,Cost_per_time,total_cost) VALUES (%s,%s,%s,%s,%s,%s);"   
        insert_in_Bill_val = (LicenseKey,Machine_Name,MAC,TOTAL_TIME_TAKEN,str(float((t1.total_seconds()/60)*5)),str(float(bill[0]) + float((t1.total_seconds()/60)*5)),) 

        queries = [insert_in_payasyougo_query,insert_in_Message_query,insert_in_Bill_query]
        values = [insert_in_payasyougo_val,insert_in_Message_val,insert_in_Bill_val]
        tables = ["PAYASYOUGO","MESSAGE","BILL"]
        for i,_ in enumerate(tables):
            executeQuery(connection,tables[i],queries[i],values[i])
        closeConnection(connection)
    except Exception as e:
        print (getattr(e, 'message', repr(e)))
        closeConnection(connection)

def sqlpath(name):
    '''This Method is use to set path to AppData/local/Lyftron

        @param name: the name of the connecter which is used for directory name
        @type name: str
        @rtype: str
        @return: path to the relative Connector
    '''

    host = platform.uname()
    Connector_path = (r'LyftronData\{name}\\').format(name=name)
    if host[0] == 'Linux':
        return os.makedirs(os.path.join(os.getenv('HOME'),Connector_path),exist_ok=True)
    if host[0] == 'Windows':
        LocalAppData = os.getenv('LOCALAPPDATA')
        path = os.path.join(LocalAppData,Connector_path)
        os.makedirs(path,exist_ok=True)
        return path
    if host[0] == 'Mac':
        pass

def readlyft(file_path):
    lst = []
    path = file_path+".lyft"
    with open(path, "rb") as file:
        for i in file.readlines():
            lst.append(str(i, 'utf-8')[::-1].lstrip("\n").split(","))
    file.close()
    with open(path , 'w') as w:
        w.truncate(0)
    w.close()
    df = pd.DataFrame(lst)
    df.to_csv(path, mode='a',index=False, header=False)
    f = open(path,"r+")
    return f
    
def readSchedule(name):
    ''' This Method is for Reading the errorSchedule of the Failed entires
        and copy it to PostgresSql Database.

        @param name: Name of the connector
        @type name: str
    '''
    global connectorname
    connectorname = name
    connection = openConnection() 
    File_Name = ["MESSAGE","BILL","PAYASYOUGO"]
    table_format = ["MESSAGE(LicenseKey,Machine_Name,MAC,MESSAGE)","BILL(LicenseKey,Machine_Name,MAC,Time_spent,Cost_per_time,total_cost)","PAYASYOUGO(LicenseKey,TOTAL_ALLOWED_TIME,REMAINING_TIME,TOTAL_TIME_SPENT,START_TIME,END_TIME,TOTAL_TIME_TAKEN,ROWS_ALLOWED,ROWS_FETCHED,MONTH,Machine_Name,MAC,IP)"]
    path = sqlpath(connectorname)
    for k,i in enumerate(File_Name):
        if not os.path.isfile(path+i+".lyft"):
            messagebox()
        else:
            f = readlyft(path+i)
            connection[0].copy_from(f, table_format[k], sep=",",null='None')
            connection[1].commit()
            f.truncate(0)
            f.close()
    closeConnection(connection)
    print("Done Reading Schedule!")
        
def writeSchedule(table,variable):
    df = pd.DataFrame([variable],index=None)
    file_path =  sqlpath(connectorname)+table+".lyft"
    for i in df.values:
        lst = ', '.join(i)
        with open(file_path, "ab") as fw:
            lst = lst[::-1]
            fw.write(bytes(lst+"\n", 'utf-8'))
            fw.close()


# Getting a tkinter message box and that is showing Some Errors and Some Data             