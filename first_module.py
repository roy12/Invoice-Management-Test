import MyToolBox

#this module get file with path and type, and a queue name and send to the second module all the details about the file
#the module do that by create an instans of queue hendler and activate the publish func with all the data
class FirstModule:
    def __init__(self,Parameters,FileFullName):
        QueueMessage = str(FileFullName+','+Parameters.table_name)
        self.QueueHendler= MyToolBox.RabbitMQ_Connector(Parameters)
        self.QueueHendler.publish(Parameters.first_queue_name,QueueMessage)
