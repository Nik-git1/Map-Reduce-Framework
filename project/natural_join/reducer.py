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
        mod=request.mod
        red=request.no_red
        for i in range(len(request.mappers)):
            file_name=str(request.mappers[i])+".txt"
            
            with open(file_name,'r') as f:
                content_list = f.readlines()

            # print the list

            # remove new line characters
            content_list = [x.strip() for x in content_list]
            # print(content_list)

            
            for s in content_list:
                n_l=s.split()
                if((len(n_l[0])%red)==mod):
                    if not n_l[0] in dic.keys():
                        dic[n_l[0]]={}

                    for i in range(1,len(n_l)):
                        temp=n_l[i].split(':')
                        tk=temp[0][1:]
                        tv=temp[1][:-1]
                        if not tk in dic[n_l[0]]:
                            dic[n_l[0]][tk]=[]
                        dic[n_l[0]][tk].append(tv)
        #     # Extract key-value pairs from the list of parts
        #     key_value_pairs = {}
        #     for i in range(0, len(pairs), 2):
        #         key_value_pairs[pairs[i]] = pairs[i+1]

        #     for k, v in key_value_pairs.items():
        #         if((len(k)%request.no_red)==request.mod):
        #             if k in dic.keys():
        #                 dic[k].append(v)
        #             else:
        #                 dic[k]=[v]
        # print(dic)
        # for i in list[0]:
        #     print(i)
        # print(list[0].values)
        final_dic={}
        for k, v in dic.items():
            final_dic[k]=[]
            for ik, iv in dic[k].items():
                final_dic[k].append(iv)
        final_list=[]
        for fk,fv in final_dic.items():
            if len(fv)==2:
                list1=fv[0]
                list2=fv[1]
                for age in list1:
                    for role in list2:
                        temp_str=fk+" " + age+", " + role
                        final_list.append(temp_str)

        print(final_list)

        out_file=str(global_port)+".txt"
        with open(out_file, "a") as f:
            for line in final_list:
                f.write(f"{line}\n")
                
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