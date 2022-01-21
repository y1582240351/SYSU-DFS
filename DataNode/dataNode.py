import os
import pickle
from concurrent import futures
import grpc
import random
import time
import json
from NameNode import nameNode_pb2_grpc, nameNode_pb2
import dataNode_pb2
import dataNode_pb2_grpc
from setting import *
import sys


class DataNode(dataNode_pb2_grpc.dataNodeServicer):
    def __init__(self, id, ip, port, root):
        self.id = id
        self.ip = ip
        self.port = port
        self.root = root
        self.path = self.root + 'file_list.json'  # 储存文件以及文件对应的key

        self.file_path = {}
        try:
            with open(self.root+'file_path.json', 'r') as fp:
                json_str = fp.read()
                fp.close()
                self.file_path = json.loads(json_str)
        except:
            self.file_path = {}

        self.file_list = {}
        try:
            with open(self.path, 'r') as fp:
                json_str = fp.read()
                fp.close()
                self.file_list = json.loads(json_str)
        except:
            self.file_list = {}

        master_channel = grpc.insecure_channel(nameNode_ip + ':' + nameNode_port)
        self.master_stub = nameNode_pb2_grpc.nameNodeStub(master_channel)
        self.master_stub.dataNodeAppend(
            nameNode_pb2.NodeAppend(
                ip=self.ip,
                port=self.port,
                keys=[int(el) for el in self.file_list.keys()],
                nid=int(self.id)
            ))
        print('DataNode ' + str(self.id) + ' is online')

    def __del__(self):
        with open(self.path, 'w') as fp:
            json_str = json.dumps(self.file_list, indent=2)
            fp.write(json_str)
            fp.close()
        with open(self.root + 'file_path.json', 'w') as fp:
            json_str = json.dumps(self.file_path, indent=2)
            fp.write(json_str)
            fp.close()
        print('DataNode' + str(self.id) + ' is offline')

    def writeLocalFile(self, iterator):
        for iter in iterator:
            key = iter.key
            filename = self.file_list[str(key)]
            with open(self.root + filename, 'wb') as fp:
                fp.write(iter.buffer)
                fp.close()
        return key

    def chunkGen(self, key):
        filename = self.file_list[str(key)]
        with open(self.root + filename, 'rb') as fp:
            while True:
                chunk = fp.read(1024 * 1024)
                if len(chunk) == 0:
                    fp.close()
                    return
                yield dataNode_pb2.ClientWriteRequest(buffer=chunk, key=key)

    def syncWrite(self, request: dataNode_pb2.ClientSyncRequest, context):
        nodes = request.nodes
        key = request.key
        if len(nodes) == 0:
            return dataNode_pb2.ClientRespond(state=1)
        # 向nodes中的一个点更新信息
        node = nodes[-1]
        nodes.pop()

        channel = grpc.insecure_channel(node.ip + ':' + node.port)
        stub = dataNode_pb2_grpc.dataNodeStub(channel)
        chunk_generator = self.chunkGen(key)
        try:
            stub.create(
                dataNode_pb2.ClientCreateRequest(
                    key=key,
                    file_name=self.file_list[str(key)],
                    path=self.file_path[str(key)],
                )
            )
            response = stub.write(chunk_generator)
            stub.syncWrite(
                dataNode_pb2.ClientSyncRequest(
                    key=key,
                    nodes=nodes,
                )
            )
            dataNode_pb2.ClientRespond(state=1)
        except Exception as err:
            print(err)
            return dataNode_pb2.ClientRespond(state=0)

    # 与client的交互

    def read(self, request: dataNode_pb2.ClientReadRequest, context):
        print('DataNode ' + str(self.id) + ' : begin transition')
        filename = self.file_list[str(request.key)]
        with open(self.root + filename, 'rb') as fp:
            while True:
                chunk = fp.read(1024 * 1024)
                if len(chunk) == 0:
                    fp.close()
                    return
                yield dataNode_pb2.Chunk(buffer=chunk)

    def write(self, request: dataNode_pb2.ClientWriteRequest, context):
        try:
            key = self.writeLocalFile(request)
            channel = grpc.insecure_channel(nameNode_ip + ':' + nameNode_port)
            stub = nameNode_pb2_grpc.nameNodeStub(channel)
            stub.dataNodeFinish(
                nameNode_pb2.NodeFinish(
                    action=int(2),
                    path=self.file_path[str(key)],
                    key=key,
                    nid=int(self.id),
                )
            )
            state = 1
            print('DataNode' + str(self.id) + ' : Successfully save file ', self.file_list[str(key)])
        except Exception as err:
            print(err)
            state = 0
        return dataNode_pb2.ClientRespond(state=state)

    def deleteFile(self, request: dataNode_pb2.ClientDeleteRequest, context):
        filename = self.file_list[str(request.key)]
        try:
            if os.path.exists(self.root + filename):
                os.remove(self.root + filename)
            print('DataNode' + str(self.id) + 'Successfully delete file ', filename)
            state = 1
        except:
            state = 0
        return dataNode_pb2.ClientRespond(state=state)

    def create(self, request: dataNode_pb2.ClientCreateRequest, context):
        self.file_list[str(request.key)] = request.file_name
        self.file_path[str(request.key)] = request.path
        print('DataNode' + str(self.id) + ' : begin creating file '
              , self.file_list[str(request.key)])
        return dataNode_pb2.ClientRespond(state=1)


def runServer(id, ip, port):
    root = './serverData/DataNode_%s/'%(id)
    if not os.path.exists(root):
        os.makedirs(root)

    data_node = DataNode(id, ip, port, root)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    dataNode_pb2_grpc.add_dataNodeServicer_to_server(data_node, server)
    server.add_insecure_port(f'[::]:{data_node.port}')
    server.start()
    try:
        while True:
            time.sleep(60*60)
    except KeyboardInterrupt:
        data_node.__del__()
        server.stop(0)

if __name__ == "__main__":
    # runServer(sys.argv[1], sys.argv[2], sys.argv[3])
    print('please input your id, ip, post: ')
    id = input('id: ')
    ip = input('ip: ')
    port = input('port: ')
    runServer(id, ip, port)
