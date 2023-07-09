import grpc 
import disc_pb2_grpc
import disc_pb2
import threading
import time
from google.protobuf.timestamp_pb2 import Timestamp
dic={}
def mapper(file, port):
    channel = grpc.insecure_channel('localhost:'+str(port))
    stub = disc_pb2_grpc.RegisterReplicaServiceStub(channel)
    register_request = disc_pb2.str(s=file)
    register_response = stub.mapper(register_request)

def reducer(mappers, no_red, mod, port):
    channel = grpc.insecure_channel('localhost:'+str(port))
    stub = disc_pb2_grpc.RegisterReplicaServiceStub(channel)
    register_request = disc_pb2.red(mappers=mappers, no_red=no_red, mod=mod)
    register_response = stub.reducer(register_request)

def run():

    global dic
    # INPUT
    
    n=int(input("enter no of files: "))
    files=[]
    for i in range(n):
        file_in=input("enter file name: ")
        files.append(file_in)
    mappers=[]
    for i in range(n):
        map_in=input("enter mapper ports: ")
        mappers.append(map_in)
    r_n=int(input("enter no of reducers: "))
    reducers=[]
    for i in range(r_n):
        red_in=input("enter reducer ports: ")
        reducers.append(red_in)

    # file=input()
    
    # map=input()
    # file='input.txt'
    # mappers=int(input())
    


    # MAPPER
    mapper_threads = []
    for i in range(n):
        t = threading.Thread(target=mapper, args=(files[i], mappers[i]))
        mapper_threads.append(t)
        t.start()
    #     keys=register_response.keys
    #     values=register_response.values
    #     print(register_response)
    #     print(keys)   
    #     print(values)
    #     i=0
    #     for s in keys:
    #         if s in dic.keys():
    #             dic[s].append(values[i])
    #         else:
    #             dic[s]=[values[i]]

    #         i+=1



    # print(dic)


    time.sleep(2)



    #  REDDUCER
    # list1 = [] 
    # k=[]
    # for key, value in dic.items():
    #     list1.append(value)
    #     k.append(key)
    # # for s in keys:
    # #     print(s)
    # #     print(dic[s])
    # #     list1.append(dic[s])
    
    # list_messages = []
    # for sublist in list1:
    #     list_message = disc_pb2.list()
    #     list_message.values.extend(sublist)
    #     list_messages.append(list_message)

    # # create an instance of the listoflist message type
    # listoflist_message = disc_pb2.listoflist()
    # listoflist_message.l.extend(list_messages)
    reducer_threads = []
    for i in range(r_n):
        t = threading.Thread(target=reducer, args=(mappers, int(r_n), i, reducers[i]))
        reducer_threads.append(t)
        t.start()
    

       
if __name__ == '__main__':
    run()        



# python3 -m grpc_tools.protoc -I protos --python_out=. --grpc_python_out=. protos/disc.proto