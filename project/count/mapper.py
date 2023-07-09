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
    def mapper(self, request, context):
        print(request)
        # Open the input file
        with open(request.s, 'r') as f:
            # Read the contents of the file
            contents = f.read()

        # Split the contents of the file into words
        words = contents.split()

        # Create an empty dictionary to store the count of each word
        word_count = {}

        # Loop through each word in the list and update the count of that word in the dictionary
        for word in words:
            if word in word_count:
                word_count[word] += 1
            else:
                word_count[word] = 1

        # Print the count of each word in the dictionary
        value=[]
        key=[]
        for word, count in word_count.items():
            key.append(word)
            value.append(count)
        file_name=str(global_port)+".txt"
        with open(file_name, "a") as f:
            for k, val in word_count.items():
                f.write(f"{k} {val}\n")


        response= disc_pb2.StringInt32Dictionary(values=value,keys=key)
        return response

def main(port):   
    global global_port
    print("server started")
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