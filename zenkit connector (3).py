#!/usr/bin/env python
# coding: utf-8

# In[1]:


import  re
from dateutil.parser import parse
import pandas as pd                                                        
from sqlalchemy import create_engine
from authlib.integrations.requests_client import OAuth2Session
import datetime
import requests
import time
import sqlparse
import os
import json
import math 
import schedule
import payasyougo as pg


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

global Connector_name
Connector_name = 'Zenkit'

data = []
gs = None
dt = None
lookUp = None
engine = create_engine('sqlite://')
engines = [['PostgreSQL','postgresql://scott:tiger@localhost/mydatabase'],['Sql Server','mssql+pyodbc://scott:tiger@mydsn'],['Oracle','oracle://scott:tiger@127.0.0.1:1521/sidname'],['MySQL','mysql://scott:tiger@localhost/foo'],['SQLite','sqlite:///foo.db']]
supportedEngines = pd.DataFrame(columns=['Engine Name','Example Connection'],data=engines,index=None)

def initializeAPI(api_key):
    StartTime = time.time()
    global token
    token= api_key
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,initializeAPI.__name__,StartTime,EndTime,Connector_name)
    return token

def req(url): #Initialize Api
        payload = "{}"
        header = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        req = requests.request("GET", url, headers=header)
        if not req:
            return {}
        else: 
            return req.json()
        

def fetchDataFromAPI(table):
    StartTime = time.time()
    bg_url= 'https://base.zenkit.com/api/v1/backgrounds'
    bg_df_url= 'https://base.zenkit.com/api/v1/backgrounds/default'
    ele_cat_url= 'https://base.zenkit.com/api/v1/elementcategories'
    user_access= 'https://base.zenkit.com/api/v1/users/me/accesses'
    l_w_ws_access= 'https://base.zenkit.com/api/v1/users/me/lists-without-workspace-access'
    new_not_min_for='https://base.zenkit.com/api/v1/users/me/new-notifications-in-minimal-format'
    noti = 'https://base.zenkit.com/api/v1/users/me/notifications'
    current_user= 'https://base.zenkit.com/api/v1/users/me'
    ws_list= 'https://base.zenkit.com/api/v1/users/me/workspacesWithLists'
    


    if table == 'background':
        background=req(bg_url)
        if not background:
            pass
        else:
            background=parse(background)
            a =background['main']
            b= background['resourceTags']
            send_data={'background': a, 'background_resourceTags': b}
            return send_data
    elif table=='default_background':
        background_def=req(bg_df_url)
        if not background_def:
            pass
        else:
            background_def=parse(background_def)
            b=background_def['main']
            a= background_def['resourceTags']
            send_data={'default_background' : b , 'def_background_resourceTags': a}
            return send_data
    elif table=='element_categories':
        ele_cat=req(ele_cat_url)
        if not ele_cat:
            pass
        else:
            ele_cat=parse(ele_cat)
            a5= ele_cat['filterKeys']
            a4= ele_cat['businessDataDefinition']
            a1= ele_cat['businessDataDefaults']
            a2= ele_cat['elementDataDefinition']
            a3= ele_cat['elementDataDefaults']
            a= ele_cat['main']
            send_data={'element_categories': a, 'businessDataDefaults' : a1 ,                       'elementDataDefinition' : a2 , 'elementDataDefaults': a3 ,                       'businessDataDefinition': a4 , 'filterKeys': a5}
            return send_data
    elif table=='user_access':
        user_access=req(user_access)
        if not user_access:
            pass
        else:
            user_access=parse(user_access)
            a=user_access['main']
            send_data={'user_access': a }
            return send_data
    elif table=='lists-without-workspace-access':
        a=req(l_w_ws_access)
        if not a:
            pass
        else:
            a=parse(a)
            a1=a['main']
            a2=a['settings']
            a3=a['emails']
            
            send_data={'lists_without_workspace_access': a1 , 'l_w_ws_access_settings': a2 , 'l_w_ws_access_emails': a3}
            return send_data
    elif table=='new-notifications-in-minimal-format':
        b=req(new_not_min_for)
        if not b:
            pass
        else:
            b=parse(b)
            a=b['main']
            send_data={ table : a }
            return send_data
    elif table=='notifications':
        c=req(noti)
        print(c)
        if not c:
            null= dict()
            return null
        else:
            c=parse(c)
            a=c['main']
            a1=c['notifications']
            send_data={'notifications': a, "notifications1": a1 }
            return send_data
    elif table=='current_user':
        d=req(current_user)
        if not d:
            pass
        else:
            d=parse(d)
            a1=d['main']
            a2=d['settings']
            a3=d['emails']
            send_data={'current_user': a1 , 'current_user_settings': a2 , 'current_user_emails': a3}
            return send_data   
    elif table=='workspacesWithLists':
        e=req(ws_list)
        if not e:
            pass
        else:
            e=parse(e)
            a=e['main']
            send_data={'workspacesWithLists': a }
            return send_data
    else:
        print("please provide the right table name")
        exit(0)
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,fetchDataFromAPI.__name__,StartTime,EndTime,Connector_name)


def parse(jsn):
    tble={}
    simple={}
    if type(jsn) == list:
        for i in jsn:
            for k, v in i.items():
                if v== 'true':
                    v= True
                elif v== 'false':
                    v = False
                else: pass
                if type(v) == list:
                    tble[k]= v
                if type(v) == dict:
                    tble[k]= v
                elif type(v) == str or type(v)== int or type(v)== bool:
                    simple[k]=v
            tble['main']= simple
            return tble
    elif type(jsn) == dict:
        for i in jsn.keys():
            print(i)
            for k, v in jsn.items():
                if v== 'true':
                    v= True
                elif v== 'false':
                    v = False
                else: pass
                if type(v) == list:
                    tble[k]= v
                if type(v) == list:
                    tble[k]= v
                elif type(v) == str or type(v)== int or type(v)== bool:
                    simple[k]=v
            tble['main']= simple
            return tble


def querymaker(query):
    columnsPart=[] # where clause value
    select=[]
    subselect=[] # sub query select
    subcol=[]  # sub query col
    col=[]
    table=[]
    updated=[]
    limit=[]
    new=[]
    offset=[]
    subtable=[] # sub query table
    parsed = sqlparse.parse(query)
    stmt=parsed[0]
    where=str(stmt.tokens[-1])

    if query.__contains__("Describe"):
        t =query.split("Describe ")
        return sysQueries(".", t[1])

    if re.search('select', where, re.IGNORECASE):# for sub query
        columnsPart = where.split('in ')[1]
        columnsPart=(columnsPart.replace('(',''))
        columnsPart=(columnsPart.replace(')',''))      
        parse=sqlparse.parse(columnsPart)
        stmts=parse[0]
        subselect=str(stmts.tokens[0])
        subcol=str(stmts.tokens[2])
        subtable=str(stmts.tokens[6])
        select=str(stmt.tokens[0])
        col=str(stmt.tokens[2])
        table=str(stmt.tokens[6])

        if query.lower().__contains__("sys."):
            return sysQueries(table, col)
            
        else:
            jsonres = fetchDataFromAPI(table)
            print(jsonres)

        list_df = json_to_df(jsonres,table)
        for df_name,df in list_df.items():
            dataToTable(df, df_name)
        r = engine.execute(query)
        tble = pd.DataFrame(r, index=None, columns=r.keys())
        return tble

    elif (re.search('=', where)):     # for query which has where clause 
        select=str(stmt.tokens[0])
        col=str(stmt.tokens[2])
        table=str(stmt.tokens[6])
        where=str(stmt.tokens[8])
        where=where.split(" ")
        for i in where:
            if i=='=':
                where.remove(i)
            elif i=='or':
                where.remove(i)
            elif i=='and':
                where.remove(i)

        where.pop(0)
        for j in where:
            if(re.search('=',j, re.IGNORECASE)):
                t=j
                where.remove(t)
                j = j.replace('=',' ')
                where.append(j)

        if query.lower().__contains__("sys."):
            return sysQueries(table, col)
            
        else:
            jsonres = fetchDataFromAPI(table)
            print(jsonres)

        list_df = json_to_df(jsonres,table)
        print(list_df)
        for df_name,df in list_df.items():
            dataToTable(df, df_name)
        r = engine.execute(query)
        tble = pd.DataFrame(r, index=None, columns=r.keys())
        return tble

    else:
        parsed = sqlparse.parse(query)
        stmt=parsed[0]
        select=str(stmt.tokens[0])
        col=str(stmt.tokens[2])
        table=str(stmt.tokens[6])
        jsonres = None

        if query.lower().__contains__("sys."):
            return sysQueries(table, col)
            
        else:
            jsonres = fetchDataFromAPI(table)
#             print(jsonres)
        if jsonres:
            list_df = json_to_df(jsonres,table)
    #         print(list_df)
            for df_name,df in list_df.items():
    #             print(df_name,df)
                dataToTable(df, df_name)
            r = engine.execute(query)
            tble = pd.DataFrame(r, index=None, columns=r.keys())
    #         print(tble)
            return tble
        else:
            print('No data in table')
        

def json_to_df(jsn,main_table_name):
    simple = dict()
    dataFrames = dict()
    for key , val in jsn.items():
        if type(val) == list:
            dataFrames[key] = pd.DataFrame(val)
        elif type(val) == dict:
            df = pd.DataFrame.from_dict(val, orient='index', dtype=None)
            dataFrames[key] = df.transpose()
        else:
            simple[key] = val
        #df = pd.DataFrame.from_dict(simple, orient='index', dtype=None)
        #dataFrames[main_table_name] = df.transpose()
    return dataFrames


def sysQueries(query, table):
    qury = query.split(".")
    command = qury[1]
    jsn = pd.read_json("schema.json", orient="records", typ="records")


    if command == "tables":
        for i in table:
            if i == "*":
                tablenames = jsn['Tables'].keys()
                tables = []
                tablestype = []
                for i in tablenames:
                    tables.append(i)
                    tablestype.append(jsn['Tables'][i]['datatype'])
                df = pd.DataFrame(tables, columns=["tablename"])
                df["datatype"] = tablestype
                return df
            else:
                col = jsn['Tables'][i]['columns']
                colinfo = pd.DataFrame(col)
                colinfo['tablename'] = i
                return colinfo

    elif command == "constraints":
        for i in table:
            tables = []
            fk = []
            pk = []
            colname = []
            if i == "*":
                tablenames = jsn['Tables'].keys()
                for i in tablenames:
                    if not i.__contains__("sys."):
                        cols = jsn['Tables'][i]['columns']
                        for col in cols:
                            if len(col['constraint']) > 0:
                                tables.append(i)
                                colname.append(col['column'])
                                if col['constraint'].__contains__("PRIMARY"):
                                    fk.append("NULL")
                                    pk.append(col['constraint'])
                                else :
                                    fk.append(col['constraint'])
                                    pk.append("NULL")
                df = pd.DataFrame(tables, columns=["tablename"])
                df["PK"] = pk
                df["FK"] = fk
                df["column name "] = colname
                return df
            else:
                col = jsn['Tables'][i]['columns']
                for j in col:
                    if len(j['constraint']) > 0:
                        tables.append(i)
                        colname.append(j['column'])
                        if j['constraint'].__contains__("PRIMARY KEY"):
                            fk.append("NULL")
                            pk.append(j['constraint'])
                        else :
                            fk.append(j['constraint'])
                            pk.append("NULL")
                        df = pd.DataFrame(tables, columns=["tablename"])
                        df["PK"] = pk
                        df["FK"] = fk
                        df["column name "] = colname
                        return df    
                        
    elif command == "methods":
        for i in table:
            data = []
            if i == "*":
                methodsname = jsn['Methods'].keys()
                for i in methodsname:
                    dic = dict()
                    dic['methodname'] = i
                    dic["return"] = jsn['Methods'][i]['return']
                    dic["returntype"] = jsn['Methods'][i]['returntype']
                    dic["description"] = jsn['Methods'][i]['description']
                    dic["parameters"] =jsn['Methods'][i]['parameters']
                    data.append(dic)
                df = pd.DataFrame(data)
                return df
            else:
                data = jsn['Methods'][i]['parameters']
                parameters = pd.DataFrame(data)
                parameters['tablename'] = i
                return parameters

    elif command == "logs":
        col = ["Logs"]
        return pd.DataFrame(pg.search(LicenseKey, command), columns=col)

    elif command == "delta":
        jsn = pd.read_json("schema.json", orient="records", typ="records")
        for i in table:
            tables = []
            deltafield = []
            if i == "*":
                tablenames = jsn['Tables'].keys()
                for i in tablenames:
                    if not i.__contains__("sys."):
                        cols = jsn['Tables'][i]['columns']
                        for col in cols:
                            if len(col['constraint']) > 0:
                                if col['constraint'].__contains__("PRIMARY"):
                                    tables.append(i)
                                    deltafield.append(col['column'])
                df = pd.DataFrame(tables, columns=["tablename"])
                df["deltafield"] = deltafield
                return df

    elif command == "connectionstring":
        for i in table:
            data = []
            if i == "*":
                methodsname = jsn['Methods'].keys()
                for i in methodsname:
                    dic = dict()
                    dic["target"] = i
                    dic["connectinparameters"] =  jsn['Methods'][i]['parameters']
                    data.append(dic)
                df = pd.DataFrame(data)
                return df
            else:
                data = jsn['Methods'][i]['parameters']
                parameters = pd.DataFrame(data)
                parameters['target'] = i
                return parameters

    elif command == "version":
        return print("version")

    elif command == "usage":
        #[licensekey	remaining_time	total_allowed_time	total_time_spent	start_time	end_time	total_time_taken	rows_allowed	rows_fetched	month	machine_name	mac	ip)
        col = ["Total Time Spent ", "Remaining Time", " Total Time alloted"]
        return pd.DataFrame(pg.search(LicenseKey, command), columns=col)


    elif command == "license":     
        col = ["LicenseKey", "ActivateDate", "ExpireDate", "Total Time Spent", "Remaining Time"]   
        return pd.DataFrame(pg.search(LicenseKey, command), columns=col,index=None)
    
    else:
        tables = jsn['Tables'][table]
        data = []
        constraints = []
        pk = ""
        fk = ""
        Statement = 'CREATE TABLE '+ table+'(' 
        for i in tables['columns']:
            Statement = Statement+i['column']+" "+i['datatype']+",\n"
            if i['constraint'].__contains__("PRIMARY"):
                pk = "PRIMARY KEY ("+i['column']+"),\n"
            elif i['constraint'].__contains__("FOREIGN "):
                fk = i['constraint']
            else:
                pass
        describedtable = Statement[:-2]+",\n"+pk+fk+');'

        return describedtable


def dataToTable(results,table):
    StartTime = time.time()
    d = results.to_sql(table, con=engine, if_exists='replace')     
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,dataToTable.__name__,StartTime,EndTime,Connector_name)


def check_data_type(val):

    if type(val) is int:
        datatype = "int"
        return datatype

    elif type(True) == type(val) or type(False) == type(val):
        datatype = "boolean"
        return datatype

    elif val == "":
        pass

    else:
        try:
            datetime.datetime.strptime(val, "%Y-%m-%dT%H:%M:%S")
            datatype = "datetime"
            return datatype

        except :
            datatype = "varchar(255)"
            return datatype


def scheduler_task(data, rows, file_name=""):
    StartTime = time.time()
    
    # assuming.......data's type is pandas framework.......
    global END, START, STOP

    END = START + rows

    if END > len(data):
        END = len(data)
        STOP = 1

    print(data.iloc[START: END])
    START += rows

    if len(file_name) > 1:
        data.to_csv(file_name)
    else:
        print('')
        print(data)
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,scheduler_task.__name__,StartTime,EndTime,Connector_name)


def schedule_data(sec, rows, df,filename):
    StartTime = time.time()
    print("Total rows : ", len(df))
    schedule.every(sec).seconds.do(
        lambda: scheduler_task(data=df, rows=rows, file_name=filename))  # file_name is optional

    while True:
        schedule.run_pending()
        if STOP:
            schedule.clear()
            break
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,schedule_data.__name__,StartTime,EndTime,Connector_name)


def connectEngine(username, password, server, database, tableName, df):
    StartTime = time.time()
    alchemyEngine = create_engine(
    f"postgresql+psycopg2://"+username+":"+password+"@"+server+"/"+database+", pool_recycle=3600")
    
    try:
        con = alchemyEngine.connect()
        df.to_sql(
            tableName, con, if_exists='replace')
        con.close()
        print("Data uploaded to table "+tableName)
    except Exception as e:
        print (getattr(e, 'message', repr(e)))
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,ConnectEngine.__name__,StartTime,EndTime,Connector_name)
    

def Pagination(data, number_of_page=0):
    StartTime = time.time()
    if number_of_page > 0:
        per_page = math.ceil(len(data) / number_of_page)
        total_page = number_of_page
    # else:
    #     total_page = len(data)

    count = 1
    print("Enter page number to show its data. \n")
    while True:
        print(data.iloc[((count - 1) * per_page): (count * per_page)])
        print("Showing page : ", count, " / ", total_page,"\n")
        key = input("Press 'A' or 'D' to navigate or jump to page no: ")
        if (key.isdigit()):
            page = int(key)
            if page > total_page:
                print('Page doesnot exist')
            else:
                count = page
        elif(key.lower() in ['a','d']):
            lower = key.lower()
            if lower == 'a':
                if count > 1:
                    count -= 1
            elif lower == 'd':
                if count < total_page:
                    count += 1
        else:
            print("Invalid page number")
            pass
    EndTime = time.time()
    pg.measure_execution_time(LicenseKey,Pagination.__name__,StartTime,EndTime,Connector_name)


# Done with this code to be understand    



if __name__ == '__main__':
    LicenseKey = "QWERTY-ZXCVB-6W4HD-DQCRG"
    api_key='ko0cgkhj-LQrD2QOeVhZjHc2lXezEBrU9CqKCzcRD'
    initializeAPI(api_key)
    query='select * from user_access'
    q=querymaker(query)
    print(q)


# In[ ]:


bg_url='https://base.zenkit.com/api/v1/backgrounds'
bg_df_url= 'https://base.zenkit.com/api/v1/backgrounds/default'
ele_cat_url= 'https://base.zenkit.com/api/v1/elementcategories'
user_access= 'https://base.zenkit.com/api/v1/users/me/accesses'
l_w_ws_access= 'https://base.zenkit.com/api/v1/users/me/lists-without-workspace-access'
new_not_min_for='https://base.zenkit.com/api/v1/users/me/new-notifications-in-minimal-format'
noti = 'https://base.zenkit.com/api/v1/users/me/notifications'
current_user= 'https://base.zenkit.com/api/v1/users/me'
ws_list= 'https://base.zenkit.com/api/v1/users/me/workspacesWithLists'
a=req(bg_url)
b=req(bg_df_url)
# c=req(ele_cat_url)
c=[
{
    "id": 1,
    "shortId": "rygaas5Ws",
    "uuid": "1ae99f78-2353-49a1-a94c-9eba8bc9d2c4",
    "name": "Textfield",
    "displayName": "Text",
    "group": "Control",
    "placeholderSchema": "null",
    "container": False,
    "listable": True,
    "filterable": True,
    "filterKeys": [],
    "sortKey": "text",
    "searchable": True,
    "isStatic": False,
    "minWidth": "75px",
    "width": "150px",
    "maxWidth": "null",
    "isWidthFixed": False,
    "canSet": True,
    "canAdd": True,
    "canRemove": True,
    "canReplace": True,
    "businessDataDefinition": {},
    "businessDataDefaults": {},
    "elementDataDefinition": {},
    "elementDataDefaults": {},
    "created_at": "2016-08-29T10:47:17.000Z",
    "updated_at": "2016-08-29T10:47:17.000Z",
    "deprecated_at": "null"
}
]
d= [
{
    "accessType": "Organization",
    "roleId": "listOwner"
}
]
e=[
{
    "id": 1,
    "shortId": "HkHh7dz5Zo",
    "uuid": "d9edb23d-f345-4fc9-9970-4db7a2de453f",
    "displayname": "Max Mustermann",
    "fullname": "Max Mustermann",
    "initials": "MM",
    "username": "max",
    "backgroundId": 1,
    "api_key": "null",
    "imageLink": "null",
    "anonymous": False,
    "locale": "de_DE",
    "timezone": "Europe/Berlin",
    "isSuperAdmin": True,
    "trello_token": "null",
    "settings": {},
    "isImagePreferred": True,
    "emails": [
        {
            "id": 5,
            "shortId": "H1msdz9-i",
            "uuid": "808bd9f7-v342-49ce-bf59-26159f4db546",
            "email": "mail@example.com",
            "isPrimary": True,
            "created_at": "2016-08-29T10:47:17.772+00:00",
            "updated_at": "2016-08-29T10:47:17.772+00:00",
            "deprecated_at": "null",
            "isVerified": True
        }
    ]
}
]

f=[
{}
]

g={
"thereAreMoreResults": False,
"newNotificationsCount": 1,
"notifications": [
    {}
]
}
h={
"id": 1,
"shortId": "HkHh7dz5Zo",
"uuid": "d9edb23d-f345-4fc9-9970-4db7a2de453f",
"displayname": "Max Mustermann",
"fullname": "Max Mustermann",
"initials": "MM",
"username": "max",
"backgroundId": 1,
"api_key": "null",
"imageLink": "null",
"anonymous": False,
"locale": "de_DE",
"timezone": "Europe/Berlin",
"isSuperAdmin": True,
"trello_token": "null",
"settings": {},
"isImagePreferred": True,
"emails": [
    {
        "id": 5,
        "shortId": "H1msdz9-i",
        "uuid": "808bd9f7-v342-49ce-bf59-26159f4db546",
        "email": "mail@example.com",
        "isPrimary": True,
        "created_at": "2016-08-29T10:47:17.772+00:00",
        "updated_at": "2016-08-29T10:47:17.772+00:00",
        "deprecated_at": "null",
        "isVerified": True
    }
]
}
i=[
{
    "id": 3,
    "shortId": "HkOvq9Zs",
    "uuid": "ea7891c9-7f07-418f-a2ae-a8b838012754",
    "name": "Team",
    "description": "null",
    "isDefault": False,
    "created_at": "2016-08-29T11:19:59.761Z",
    "updated_at": "2016-08-29T11:19:59.761Z",
    "deprecated_at": "null",
    "backgroundId": "null",
    "created_by": 5
}
]
a= parse(a)
b= parse(b)
c= parse(c)
d= parse(d)
e= parse(e)
f= parse(f)
g= parse(g)
h= parse(h)
i= parse(i)
print(type(c))
print(c)
# print(type(d['main']['accessType']))
print(type(d))
endpoint_keys={'background': {}}


# In[58]:


jsn={
    'background': a,
    'default_background': b ,
    'element_categories':c ,
    'user_access':d ,
    'lists-without-workspace-access' : e,
    'new-notifications-in-minimal-format': f,
    'notifications':g ,
    'current_user': h,
    'workspacesWithLists':i 
    
    
    
    
}
# print(jsn)
create_schema(jsn)


# In[43]:


def create_schema(endpoints, endpoints_keys=None):
    # get response of endpoint as dic and their keys
    jsn = {"Tables": {}}  # create dict of table with empty object

    def table_info(file, table_name, columns):
        file["Tables"].update({table_name: {"datatype": "table", "columns": columns}})

    def column_info(keys, constraint, values, description):
        datatype = check_data_type(values)
        col_info = {
            "column": keys,
            "constraint": constraint,
            "datatype": datatype,
            "description": description,
        }

        return col_info

    def addkey():
        for k, v in endpoints_keys.items():
            # print(k, v)
            try:
                datatype = check_data_type(endpoints[k][v])
                key = {
                    "column": v,
                    "constraint": f"FOREIGN KEY ({v}) REFERENCES {k}({v})",
                    "datatype": datatype,
                    "description": "",
                }
            except Exception as e:
                pass
        return key

    for endpointName, endpoint_json in endpoints.items():
        cols = []
        if endpoints_keys is not None:  # If key is given
            for i, j in endpoints_keys.items():
                if endpointName == i:
                    pass
                else:
                    cols.append(addkey())

        print("Creating schema of " + endpointName)

        if not endpoint_json:
            cols = []
            table_info(jsn, endpointName, cols)
        print(endpoint_json)
        for endpoint_key, endpoint_value in endpoint_json.items():

            if type(endpoint_value) is list:
                col = []
                for i in endpoint_value:
                    if not i:
                        col = []
                    else:
                        for key, val in i.items():
                            col.append(column_info(key, "", val, ""))
                if endpoints_keys is not None:
                    col.append(addkey())
                table_info(jsn, endpoint_key, col)

            elif type(endpoint_value) is dict:
                col = []
                for key, val in endpoint_value.items():
                    col.append(column_info(key, "", val, ""))
                if endpoints_keys is not None:
                    col.append(addkey())
                table_info(jsn, endpoint_key, col)

            else:
                if endpoints_keys is not None:
                    for i, j in endpoints_keys.items():
                        if endpointName == i and j == endpoint_key:
                            cols.append(
                                column_info(
                                    endpoint_key, "PRIMARY KEY", endpoint_value, ""
                                )
                            )
                        elif endpoint_key == j:
                            pass
                        else:
                            cols.append(
                                column_info(endpoint_key, "", endpoint_value, "")
                            )
                else:
                    cols.append(column_info(endpoint_key, "", endpoint_value, ""))
            table_info(jsn, endpointName, cols)

    with open("systemtables.json", "r") as systables:
        tables = json.load(systables)
    with open("methods.json", "r") as methods:
        methods = json.load(methods)
    with open("schema.json", "w") as f:
        jsn["Tables"].update(tables)
        jsn.update(methods)
        json.dump(jsn, f)


# In[ ]:




