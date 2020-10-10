import MyToolBox
import first_module
import second_module
import third_module
import _thread

#creating modules instans functions
def startA(Parameters,FileFullName):
    first_module.FirstModule(Parameters,FileFullName)
def startB(Parameters):
    second_module.SecondModule(Parameters)
def startC(Parameters):
    third_module.ThirdModule(Parameters)

#cleaning database func
def CleanTable(db,TableName):
    s= MyToolBox.SqliteHandler(db)
    s.CleanTable(TableName)
    s.CloseConnection()

#get parameters from file func
def GetParameters(ParameterFile):
    Parameters = None
    try:
        Parameters = MyToolBox.MyParameters(ParameterFile)
    except FileNotFoundError as error:
        print(error)
        print('wrong parameter file')
        exit()
    except FileExistsError as error:
        print(error)
        print('wrong parameter file')
        exit()
    return Parameters

#start user interface and the listeners modules func
def start():

    Parameters=GetParameters("ParameterFile.json") #get parameters
    _thread.start_new_thread(startB,(Parameters,)) #start second module listener
    _thread.start_new_thread(startC,(Parameters,)) #start third module listener

    #user interface
    flag = 0
    while(flag==0):
        print('press 1 to enter the first file \n'
              'press 2 to enter the second file \n'
              'press 3 to enter the third file \n'
              'press 4 to enter the forth file \n'
              'press 5 to enter the fifth file \n'
              'press C to clean table \n'
              'press Q to exit')
        flag = 1
        UserInput=input()
        if UserInput=='Q' or UserInput=='q':
            print('good bye')
            break
        elif UserInput== '1':
            print('start loading first file entered')
            _thread.start_new_thread(startA,(Parameters,Parameters.first_file_path,)) #start first module with the file from the parameter file
            flag=0
        elif UserInput == '2':
            print('start loading second file entered')
            _thread.start_new_thread(startA, (Parameters, Parameters.second_file_path,)) #start first module with the file from the parameter file
            flag = 0
        elif UserInput == '3':
            print('start loading third file entered')
            _thread.start_new_thread(startA, (Parameters, Parameters.third_file_path,)) #start first module with the file from the parameter file
            flag = 0
        elif UserInput == '4':
            print('start loading forth file entered')
            _thread.start_new_thread(startA, (Parameters, Parameters.forth_file_path,)) #start first module with the file from the parameter file
            flag = 0
        elif UserInput == '5':
            print('start loading fifth file entered')
            _thread.start_new_thread(startA, (Parameters, Parameters.fifth_file_path,)) #start first module with the file from the parameter file
            flag = 0
        elif UserInput=='C' or UserInput=='c':
            print('cleaning table')
            CleanTable(Parameters.db_name,Parameters.table_name) #start first module with the file from the parameter file
            flag = 0
        else:
            flag = 0
            print('wrong input')
