import pika
import sqlite3
import pandas
import matplotlib.pyplot as plt
import json
import os

#RabbitMQ handler class
class RabbitMQ_Connector():
    #constractor
    def __init__(self,Parameters):
        self.Parameters = Parameters
        self.url = Parameters.queue_url
        self.params = None
        self.connection = None
        self.channel = None
        try:
            self.params = pika.URLParameters(self.url)
            self.connection = pika.BlockingConnection(self.params)
            self.channel = self.connection.channel()
        except:
            print('can not connect to the queue, trying again')
            try:
                self.params = pika.URLParameters(self.url)
                self.connection = pika.BlockingConnection(self.params)
                self.channel = self.connection.channel()
            except:
                print('can not connect to the queue')
                exit()

    # publish a message in the queue
    def publish(self,QueueName,QueueMessage):
        self.channel.basic_publish(exchange='', routing_key=QueueName, body=QueueMessage)

    # the next to func is listener to the first queue and callback func on message receive
    def callback(self,ch, method, properties, body):
        QueueName = self.Parameters.second_queue_name
        body = self.MessageInterpater(body)
        FileDetails = FileDetail(body)


        try:
            SqlHandler = SqliteHandler(self.Parameters.db_name)
            SqlHandler.FileToSql(FileDetails.data,FileDetails.TargetTableName)
            SqlHandler.CloseConnection()
        except:
            print('problem to conect database trying again')
            try:
                SqlHandler = SqliteHandler(self.Parameters.db_name)
                SqlHandler.FileToSql(FileDetails.data, FileDetails.TargetTableName)
                SqlHandler.CloseConnection()
            except:
                print('problem to conect database')
        try:
            self.publish(QueueName,str(FileDetails.TargetTableName))
        except:
            print('database updated but queue do not got update about it,trying again')
            try:
                self.publish(QueueName, str(FileDetails.TargetTableName))
            except:
                print('database updated but queue do not got update about it,the graph will apdate after the next update')

    def ListenerToFile(self,QueueName):
        self.channel.queue_declare(queue=QueueName)
        self.channel.basic_consume(on_message_callback=self.callback, queue=QueueName, auto_ack=True)
        self.channel.start_consuming()

    # the next to func is listener to the second queue and callback func on message receive
    def callbackUpdates(self,ch, method, properties, body):
        body = self.MessageInterpater(body)
        try:
            SqlHandler = SqliteHandler(self.Parameters.db_name)
            SqlHandler.GetDataToGraph(body)
            SqlHandler.CloseConnection()
        except:
            print('problem to conect database trying again')
            try:
                SqlHandler = SqliteHandler(self.Parameters.db_name)
                SqlHandler.GetDataToGraph(body)
                SqlHandler.CloseConnection()
            except:
                print('problem to conect database, the graph will be updated afterthe next update')
    def ListenerToUpdates(self,QueueName):
        self.channel.queue_declare(queue=QueueName)
        self.channel.basic_consume(on_message_callback=self.callbackUpdates, queue=QueueName, auto_ack=True)
        self.channel.start_consuming()


    def CloseConnection(self):
        self.QueueHendler.connection.close

    def MessageInterpater(self,message):
        length = len(str(message)) - 1
        message = str(message)[2:length]
        return message

#flie handler class
class FileDetail():
    #constractor
    def __init__(self,message):
        FirstComma = message.find(',')
        self.FileFullName = message[0:FirstComma]
        self.FileType = self.GetType()
        self.TargetTableName = str(message[FirstComma + 1:])
        self.data = None
        try:
            self.data=self.GetData()
        except FileNotFoundError as error:
            print(error)
            print('can not find the file from the queue message')
            exit()
        except FileExistsError as error:
            print(error)
            print('can not find the file from the queue message')
            exit()
    #get file type from the full name
    def GetType(self):
        t, t = os.path.splitext(self.FileFullName)
        return t[1:]
    #get the data from the file
    def GetData(self):
        data=None
        if self.FileType == 'json' or self.FileType =='JSON':
            data = pandas.read_json(r'{}'.format(str(self.FileFullName)))
        elif self.FileType == 'csv' or self.FileType =='CSV':
            data = pandas.read_csv(r'{}'.format(str(self.FileFullName)))

        return data

#sqlite handler class
class SqliteHandler():
    #constractor
    def __init__(self,DB):
        self.conn = sqlite3.connect(DB)

    def CloseConnection(self):
        self.conn.close()

    #insert data that received to the table that received
    def FileToSql(self,data, TableName):
        data.to_sql(name=TableName, con=self.conn, if_exists='append', index=False)
        self.conn.commit()

    #get data from table
    def ReadData(self,TableName):
        c = self.conn.cursor()
        c.execute("select * from " +TableName)
        print(c.fetchall())
        self.conn.commit()

    #truncate table
    def CleanTable(self,TableName):
        c = self.conn.cursor()
        c.execute("delete from " +TableName)
        self.conn.commit()

    #get data from table and show it on a graph by calling the next func
    def GetDataToGraph(self, TableName):
        c = self.conn.cursor()
        c.execute(str("select strftime('%Y-%m', InvoiceDate) as dates,count(distinct CustomerId) as CustNum,sum(Total) as TotPM from "+ TableName +" group by strftime('%Y-%m', InvoiceDate)"))
        data=(c.fetchall())
        self.conn.commit()
        self.DataToGraph(data)

    #get data and show it on graph
    def DataToGraph(self,data):
        dates = list()
        customers = list()
        total = list()
        for (a, b, c) in data:
            dates.append(f"{a}")
            customers.append(float(f"{b}"))
            total.append(float(f"{c}"))
        plt.plot(dates, customers,total)
        plt.show()

#parameters file handler class
class MyParameters():
    #constractor
    def __init__(self,FileName):
        JsonData=None
        with open(FileName, "r") as reader:
            JsonData = json.load(reader)

        self.queue_url = JsonData["queue_url"]
        self.first_queue_name = JsonData["first_queue_name"]
        self.second_queue_name = JsonData["second_queue_name"]
        self.db_name = JsonData["db_name"]
        self.table_name = JsonData["table_name"]
        self.first_file_path = JsonData["first_file_path"]
        self.second_file_path = JsonData["second_file_path"]
        self.third_file_path = JsonData["third_file_path"]
        self.forth_file_path = JsonData["forth_file_path"]
        self.fifth_file_path = JsonData["fifth_file_path"]