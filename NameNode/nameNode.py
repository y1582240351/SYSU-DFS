import os
import pickle
import time
from concurrent import futures
import grpc
import random
import nameNode_pb2
import nameNode_pb2_grpc
from DataNode import dataNode_pb2, dataNode_pb2_grpc
from setting import *
import json


class NameNode(nameNode_pb2_grpc.nameNodeServicer):
    def __init__(self, ip, port, root):
        self.ip = ip
        self.port = port
        self.root = root
        self.path = self.root + 'directory.json'  # 保存文件目录的文件

        self.directory = {}
        try:
            with open(self.path, "r") as fp:
                json_str = fp.read()
                fp.close()
                self.directory = json.loads(json_str)
        except:
            self.directory = {"": {}, "key_num": 0}

        self.node_list = {}  # node->file [ id:{ip:,port:,keys:} ]

        self.read_lock_table = {}
        try:
            with open(self.root + 'r_lock.json', 'r') as fp:
                json_str = fp.read()
                self.read_lock_table = json.loads(json_str)
        except:
            self.read_lock_table = {}

        self.write_lock_table = {}
        try:
            with open(self.root + 'w_lock.json', 'r') as fp:
                json_str = fp.read()
                self.write_lock_table = json.loads(json_str)
        except:
            self.write_lock_table = {}

    def __del__(self):
        with open(self.path, 'w') as fp:
            json_str = json.dumps(self.directory, indent=2)
            fp.write(json_str)
            fp.close()
        with open(self.root + 'r_lock.json', 'w') as fp:
            json_str = json.dumps(self.read_lock_table, indent=2)
            fp.write(json_str)
            fp.close()
        with open(self.root + 'w_lock.json', 'w') as fp:
            json_str = json.dumps(self.write_lock_table, indent=2)
            fp.write(json_str)
            fp.close()

    # 更新NameNode本地的信息

    # 文件操作
    def locateFromDir(self, path):
        curr_dir = self.directory
        for sub in path:
            if sub not in curr_dir:
                return [], -1
            else:
                curr_dir = curr_dir[sub]
        if isinstance(curr_dir, list):
            return curr_dir[0], curr_dir[1]
        return [], -1

    def deleteFromDir(self, path):
        curr_dir = self.directory
        file_path, filename = path[:-1], path[-1]
        for sub in file_path:
            if sub not in curr_dir:
                return False
            else:
                curr_dir = curr_dir[sub]
        curr_dir.pop(filename)
        return False

    def addToDir(self, path, filename, nodes):
        self.directory["key_num"] += 1
        curr_key = self.directory["key_num"]
        curr_dir = self.directory
        for sub in path:
            if sub not in curr_dir:
                return False
            curr_dir = curr_dir[sub]
        curr_dir[filename] = [nodes, curr_key]
        return True

    def addDirNode(self, path, nid):
        curr_dir = self.directory
        for sub in path:
            if sub not in curr_dir:
                return False
            if isinstance(sub, list):
                sub[0].append(nid)
                return True
            else:
                curr_dir = curr_dir[sub]
        return False

    def lockFile(self, key, state):
        if state == 0:
            self.read_lock_table[str(key)] += 1
            return
        if state == 1:
            self.write_lock_table[str(key)] += 1
            return

    def unlockFile(self, key, state):
        if state == 0:
            self.read_lock_table[str(key)] -= 1
            return
        if state == 1:
            self.write_lock_table[str(key)] -= 1
            return

    def createFile(self, path, filename, nid, key):
        if self.directory["key_num"] >= key:
            # 文件目录中还没有该文件，在目录中添加文件
            self.addDirNode(path, nid)
        else:
            # 文件已经添加进文件路径中了，这时只需要更新文件中对应节点的下标即可
            self.addToDir(path, filename, [nid])
        return True

    # 文件目录的操作
    def getDir(self, path):
        curr_dir = self.directory
        for sub in path:
            if sub not in curr_dir:
                return None
            else:
                curr_dir = curr_dir[sub]
        return curr_dir

    def addDir(self, path, new_dir):
        curr_dir = self.getDir(path)
        curr_dir[new_dir] = {}

    # 与DataNode的proto交互

    def dataNodeAppend(self, request: nameNode_pb2.NodeAppend, context):
        nid = request.nid
        new_node = {"ip": request.ip, "port": request.port, "keys": request.keys}
        self.node_list[nid] = new_node
        return nameNode_pb2.FinishACK(state=1)

    def dataNodeLeave(self, request: nameNode_pb2.NodeLeave, context):
        nid = request.nid
        self.node_list.pop(nid)
        return nameNode_pb2.FinishACK(state=1)

    def dataNodeFinish(self, request: nameNode_pb2.NodeFinish, context):
        action = request.action
        # 成功写入文件
        if action == 1:
            return nameNode_pb2.NodeFinish(state=1)
        # 成功创建文件
        if action == 2:
            norm_path = os.path.normpath(request.path)
            path = norm_path.split(os.sep)
            self.write_lock_table[str(request.key)] = 0
            self.read_lock_table[str(request.key)] = 0
            self.createFile(path[:-1], path[-1], request.nid, request.key)
            return nameNode_pb2.FinishACK(state=1)
        return nameNode_pb2.FinishACK(state=0)

    def deleteFile(self, path, key, nodes):
        try:
            for node in nodes:
                channel = grpc.insecure_channel(node.ip + ':' + node.port)
                stub = dataNode_pb2_grpc.dataNodeStub(channel)
                response = stub.deleteFile(dataNode_pb2.ClientDeleteRequest(key=key))
            self.deleteFromDir(path)
            print("NameNode: Successfully delete file ", path)
            state = True
        except:
            state = False
        return state

    def ls(self, request: nameNode_pb2.ClientLsRequest, context):
        norm_path = os.path.normpath(request.path)
        path = norm_path.split(os.sep)
        # os.path.split在解析'/'时会出问题
        if request.path == '/':
            path = ['']
        curr_dir = self.getDir(path)
        if curr_dir is None:
            print("NameNode: Error in \'ls\': Can't find the dir ", norm_path)
            return nameNode_pb2.ClientLsResponse(objects=[])
        print("NameNode: \'ls\' success")
        return nameNode_pb2.ClientLsResponse(objects=curr_dir.keys())

    def cd(self, request: nameNode_pb2.ClientCdResponse, context):
        norm_path = os.path.normpath(request.path)
        path = norm_path.split(os.sep)
        curr_dir = self.getDir(path)
        if curr_dir is None:
            print("NameNode: Error in \'cd\': Can't find the dir ", norm_path)
            return nameNode_pb2.FinishACK(state=0)
        else:
            print("NameNode: \'cd\' success")
            return nameNode_pb2.FinishACK(state=1)

    def mkdir(self, request: nameNode_pb2.ClientMkdirRequest, context):
        norm_path = os.path.normpath(request.path)
        path = norm_path.split(os.sep)
        curr_dir = self.getDir(path[:-1])
        if curr_dir is None:
            print("NameNode: Error in \'mkdir\': Can't find the path ", norm_path)
            return nameNode_pb2.FinishACK(sate=0)
        print("NameNode: \'mkdir\' success")
        curr_dir[path[-1]] = {}
        return nameNode_pb2.FinishACK(state=1)

    # 与用户节点的proto交互
    def getDataNode(self, request: nameNode_pb2.ClientFileRequest, context):
        norm_path = os.path.normpath(request.path)
        path = norm_path.split(os.sep)
        action = request.action
        nodes, key = [], -1
        if action != 2:
            nodes, key = self.locateFromDir(path)
            # 如果文件不存在，直接返回失败
            if key == -1:
                return nameNode_pb2.ClientFileRespond(state=0)
            # 如果文件正在被其他用户读取，直接返回失败
            # 如果是创建文件，则不需要加锁
            if self.write_lock_table[str(key)] != 0:
                return nameNode_pb2.ClientFileRespond(state=0)

        # 处理用户读文件
        if action == 0:
            self.lockFile(key, 0)
            ret_nodes = []
            for i in nodes:
                ret_nodes.append(
                    nameNode_pb2.dataNode(
                        id=i,
                        ip=self.node_list[i]["ip"],
                        port=self.node_list[i]["port"]
                    )
                )
            return nameNode_pb2.ClientFileRespond(state=1, nodes=ret_nodes, key=key)

        # 处理用户写
        if action == 1:
            # 如果此时有用户正在读该文件，则拒绝执行写操作。这种做法有可能会造成写饥饿
            if self.read_lock_table[str(key)] != 0:
                return nameNode_pb2.ClientFileRespond(state=0)
            self.lockFile(key, 1)
            ret_nodes = []
            for i in nodes:
                ret_nodes.append(
                    nameNode_pb2.dataNode(
                        id=i,
                        ip=self.node_list[i]["ip"],
                        port=self.node_list[i]["port"]
                    )
                )
            return nameNode_pb2.ClientFileRespond(state=1, nodes=ret_nodes, key=key)

        # 处理用户删除文件
        if action == -1:
            # 有用户正在读文件，禁止删除文件
            if self.read_lock_table[str(key)] != 0:
                return nameNode_pb2.ClientFileRespond(state=0)
            self.lockFile(key, 1)
            if self.deleteFile(path, key, nodes):
                return nameNode_pb2.ClientFileRespond(state=1)
            else:
                return nameNode_pb2.ClientFileRespond(state=0)

        # 处理用户创建文件
        if action == 2:
            key = self.directory["key_num"] + 1  # 文件的新编号
            ret_nodes = random.sample(self.node_list.keys(), int((len(self.node_list) + 1)/2))
            if len(ret_nodes) != 0:
                for i in ret_nodes:
                    nodes.append(
                        nameNode_pb2.dataNode(
                            id=i,
                            ip=self.node_list[i]["ip"],
                            port=self.node_list[i]["port"]
                        )
                    )
                return nameNode_pb2.ClientFileRespond(state=1, nodes=nodes, key=key)
            else:
                return nameNode_pb2.ClientFileRespond(state=-1)

        return nameNode_pb2.ClientFileRespond(state=0)

    def wave(self, request: nameNode_pb2.ClientFinish, context):
        action = request.action
        key = request.key
        if action == 0:
            self.unlockFile(key, 0)
            return nameNode_pb2.FinishACK(state=1)
        if action == 1:
            self.unlockFile(key, 1)
            return nameNode_pb2.FinishACK(state=1)
        if action == -1:
            self.unlockFile(key, 1)
            return nameNode_pb2.FinishACK(state=1)
        if action == 2:
            return nameNode_pb2.FinishACK(state=1)
        return nameNode_pb2.FinishACK(state=0)


def runServer():
    root = './serverData/nameNode/'
    if not os.path.exists(root):
        os.makedirs(root)
    name_node = NameNode(nameNode_ip, nameNode_port, root)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    nameNode_pb2_grpc.add_nameNodeServicer_to_server(name_node, server)
    server.add_insecure_port(f'localhost:{name_node.port}')
    server.start()
    try:
        while True:
            time.sleep(60 * 60 * 24)
    except KeyboardInterrupt:
        name_node.__del__()
        server.stop(0)


if __name__ == "__main__":
    runServer()
