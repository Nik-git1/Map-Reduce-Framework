import grpc 
import  disc_pb2_grpc
import  disc_pb2
import socket
import sys
import os 
from concurrent import futures
import time
import os
from os.path import exists
from google.protobuf.timestamp_pb2 import Timestamp
global_port=""
class RegisterReplicaServiceServicer(disc_pb2_grpc.RegisterReplicaServiceServicer):
    global global_port
    def reducer(self, request, context):
        print(request)
        # list=request.l
        # keys=request.keys
        dic={}
        for i in range(len(request.mappers)):
            file_name=str(request.mappers[i])+".txt"
            with open(file_name, 'r') as f:
                input_str = f.read()
            pairs = input_str.split()

        #     # Extract key-value pairs from the list of parts
            key_value_pairs = {}
            for i in range(0, len(pairs), 2):
                key_value_pairs[pairs[i]] = pairs[i+1]

            for k, v in key_value_pairs.items():
                if((len(k)%request.no_red)==request.mod):
                    if k in dic.keys():
                        dic[k].append(v)
                    else:
                        dic[k]=[v]
        print(dic)
        # for i in list[0]:
        #     print(i)
        # print(list[0].values)
        

        out_file=str(global_port)+".txt"
        with open(out_file, "a") as f:
            for key in dic:
                f.write(f"{key}: ")
                for s in dic[key]:
                    f.write(f"{s}  ")
                f.write("\n")
        response= disc_pb2.void()
        return response

def main(port):   
    print("server started")
    global global_port
    global_port=port
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    disc_pb2_grpc.add_RegisterReplicaServiceServicer_to_server(RegisterReplicaServiceServicer(),server)
    server.add_insecure_port('localhost:'+str(port))
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python server.py <port>')
        sys.exit(1)
    main(int(sys.argv[1]))