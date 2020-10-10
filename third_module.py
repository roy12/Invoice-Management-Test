import MyToolBox

#the third module wait for message from the second module about updates in the table
#when it gets the message it open the table and show graph of al the data (updated)
#the module do that by create an instans of queue hendler and activate the listener func
class ThirdModule:
    def __init__(self,Parameters):
        self.QueueHendler = MyToolBox.RabbitMQ_Connector(Parameters)
        self.QueueHendler.ListenerToUpdates(Parameters.second_queue_name)