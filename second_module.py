import MyToolBox

#the second module wait for message from the first module with file details
#when it gets the message it open the file and insert all the data to the database
#after that it send message in another queue to the third module
#the module do that by create an instans of queue hendler and activate the listener func
class SecondModule:

    def __init__(self,Parameters):
        self.QueueHendler= MyToolBox.RabbitMQ_Connector(Parameters)
        self.QueueHendler.ListenerToFile(Parameters.first_queue_name)

