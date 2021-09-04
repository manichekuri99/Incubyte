import pandas as pd
import mysql.connector as mysql
import time
from mysql.connector import Error
import tkinter as tk
from tkinter import *
from tkinter import ttk


class Connector:
    def __init__(self,host,password):
        self.host = host
        self.passwd= password
        self.conn = None
        self.connect()
    def connect(self):
        self.conn = mysql.connect(host = "localhost", user = self.host,password = self.passwd,auth_plugin='mysql_native_password',connect_timeout=288000)
        # print("*** Connection to the DataBase is initilized ***")
    
    def end_connection(self):
        self.conn.close()
        print("*** Connection to the DataBase Ended ***")


class Database:
    def __init__(self,connector):
        self.conn = connector
        self.data = None
        
    def init__database(self):
        cursor = self.conn.cursor()
        try:
            cursor.execute("DROP DATABASE IF EXISTS HOSPITAL")
        except Error as err:
            print("Error while DROPING HOSPITAL to MySQL", err)
            
        
        cursor.close()
        cursor = self.conn.cursor()
        sql = "CREATE DATABASE IF NOT EXISTS HOSPITAL"
        try:
            cursor.execute(sql)
        except Error as err:
            print("Error while CREATING HOSPITAL to MySQL", err)
            
        cursor.close()
            
    def create_table(self,name):
        sql = "CREATE TABLE "+ name+" (Customer_Name VARCHAR(255) NOT NULL, Customer_Id VARCHAR(18) NOT NULL,Open_Date INT NOT NULL,Last_Consulted_Date INT,Vaccination_Id VARCHAR(5),Dr_Name VARCHAR(255),State CHAR(5),Country CHAR(5),DOB INT,Is_Active CHAR(1))"
        cursor = self.conn.cursor()
        try:
            cursor.execute("USE HOSPITAL")
            cursor.execute("DROP TABLE IF EXISTS "+name)
            cursor.execute(sql)
        except Error as err:
            print("Error while CREATING The Table", err)
            
        cursor.close()
        
    def Load_Data(self,data,name):
        start = time.time()
        tpls = [tuple(x) for x in data.to_numpy()]
        sql = 'INSERT INTO '+name+' ('
        for i in range(len(tuple(data.columns))):
            val = tuple(data.columns)[i]
            val = val.replace("'","")
            sql = sql+val+", "
            
#         sql = 'INSERT INTO '+name+str(tuple(df.columns))+' VALUES '+vals
        cursor = self.conn.cursor()
        
        for i in tpls:
            que = sql[:-2]+') VALUES '+str(i)
            # print(que)
            try:
                cursor.execute("USE HOSPITAL")
                cursor.execute(que)
            except Error as e:
                print("Error while inserting to MySQL", e)

            
        end = time.time()
        cursor.close()
        # print("Time taken to upload the data of",name,(end-start),"seconds")
        
        
    def Query(self,name):
        sql = "select *  from "+name
        cursor = self.conn.cursor()
        ret_data = None
        # fstart = time.time()
        try:
            cursor.execute("USE HOSPITAL")
            cursor.execute(sql)
            ret_data = cursor.fetchall()
        except Error as err:
            print("Error while CREATING The Table", err)
            cursor.close() 

        cursor.close()
        print(ret_data)
    
    
    
    
def Pre_process(filename):
    myfile = open(filename,'r')
    header = myfile.readline().rstrip()
    head = header.split("|")
    head = head[1:]
    # print(head)
    data = []
    while myfile:
        line  = myfile.readline().rstrip()
        line = line.split("|")[1:]
    #     print(line)
        if len(line) != 0:
            data.append(line)
        else:
            break
            
    myfile.close() 

    df = pd.DataFrame(data, columns = head)
    df[["Open_Date", "Last_Consulted_Date","DOB"]] = df[["Open_Date", "Last_Consulted_Date","DOB"]].apply(pd.to_numeric)
    return df



m = tk.Tk()
Filename = tk.StringVar(m)
m.geometry("800x200")
m.title("Enter Filename")
button1 = tk.Button(m,activebackground="blue", text='Submit', width=25, command=m.destroy)
Label(m, text='Filename').grid(row=0)
e1 = Entry(m,textvariable=Filename,width=50)
e1.grid(row=0, column=1) 
button1.grid(row=1)
m.mainloop()

data_frame = Pre_process(Filename.get())

# print(data_frame)


m = tk.Tk()
m.geometry("800x200")
Username = tk.StringVar(m)
Password = tk.StringVar(m)
m.title("Connection to Mysql Server Enter Username and password")

button1 = tk.Button(m,activebackground="blue", text='Submit', width=50, command=m.destroy)
Label(m, text='Username').grid(row=0)
e1 = Entry(m,textvariable=Username,width=50)
Label(m,text="Password").grid(row=1)
e2 = Entry(m,textvariable=Password,width=50)
e1.grid(row=0, column=1)
e2.grid(row=1, column=1)
button1.grid(row=2)
# button2 = tk.Button(m,activebackground="blue", text='submmit', width=25, command=)
# button2.grid(row=1,column=1)
m.mainloop()

Conn =Connector(Username.get(),Password.get())

d = Database(Conn.conn)

data_frame.drop('H',axis='columns', inplace=True)
d.init__database()
# print("Database Created")
coun = data_frame.Country.unique()
for c in coun:
    df1 = data_frame.loc[data_frame['Country']==str(c)].copy()
    name = "Table_"+str(c)
    # df1.drop('Country',axis='columns', inplace=True)
    d.create_table(name)
    # print("Created Table",name)
    d.Load_Data(df1,name)
    # print("Loaded Data into Table")
# Database_create(d,data_frame)

def Query(name,DB):
    n = "Table_"+str(name)
    d.Query(n)


m = tk.Tk()
m.geometry("800x200")
Query_coun = tk.StringVar(m)
m.title("Query Table Based on Country")
Coun_str = "The Country Present are "+ str(coun)
Label(m, text=Coun_str,width=80).grid(row=0)
e1 = Entry(m,textvariable=Query_coun,width=50)
Label(m,text="Query_country").grid(row=1)
e1.grid(row=1, column=1)
button1 = tk.Button(m,activebackground="blue", text='Query', width=50, command=lambda: Query(Query_coun.get(),d))
button1.grid(row=2)
m.mainloop()
