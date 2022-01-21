import os
import grpc
from setting import *
from NameNode import nameNode_pb2, nameNode_pb2_grpc
from DataNode import dataNode_pb2, dataNode_pb2_grpc


class Client:
    def __init__(self, id, root):
        self.id = id
        self.root = root
        self.curr_path = '/'
        self.open_list = []
        self.connectNameNode()
        self.openKey = []



    def connectNameNode(self):
        channel = grpc.insecure_channel(nameNode_ip + ':' + nameNode_port)
        self.main_stub = nameNode_pb2_grpc.nameNodeStub(channel)

    # 文件相关操作

    def saveChunk(self, chunks, path):
        file_path, file_name = os.path.split(path)
        file_path = os.path.normpath(file_path)
        path = os.path.normpath(path)
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        with open(path, 'wb') as fp:
            for chunk in chunks:
                fp.write(chunk.buffer)

    def getChunk(self, src_path, key):
        with open(src_path, 'rb') as fp:
            while True:
                chunk = fp.read(1024 * 1024)
                if len(chunk) == 0:
                    return
                yield dataNode_pb2.ClientWriteRequest(key=key, buffer=chunk)

    def uploadFile(self, filename, action):
        # src_path = self.root + self.curr_path + filename
        src_path = self.root + filename
        origin_path = self.curr_path + filename

        if not os.path.exists(src_path):
            print("System: files does not exit")
            return

        response = self.main_stub.getDataNode(
            nameNode_pb2.ClientFileRequest(
                path=origin_path,
                action=action,  # 1:写 2:创建
            )
        )
        if response.state == 0:
            print("System: The file is locked")
            return
        if response.state == -1:
            print("System: Upload file error!!")
            return

        if len(response.nodes) == 0:
            # print("System: Can't upload the file")
            return

        node = response.nodes[-1]
        response.nodes.pop()
        channel = grpc.insecure_channel(node.ip + ':' + node.port)
        stub = dataNode_pb2_grpc.dataNodeStub(channel)
        stub.create(
            dataNode_pb2.ClientCreateRequest(
                key=response.key,
                file_name=filename,
                path=origin_path,
            )
        )
        chunk_generator = self.getChunk(src_path, response.key)
        try:
            response_data = stub.write(chunk_generator)
            if response_data.state == 1:
                nodes = []
                for node in response.nodes:
                    nodes.append(
                        dataNode_pb2.dataNodeInfo(
                            id=node.id,
                            ip=node.ip,
                            port=node.port,
                        )
                    )
                stub.syncWrite(
                    dataNode_pb2.ClientSyncRequest(
                        nodes=nodes,
                        key=response.key,
                    )
                )
                print("System: success to upload file: ", src_path)
            else:
                print("System: fail to upload file: ", src_path)
        except Exception as err:
            print(err)

        response_finish = self.main_stub.wave(
            nameNode_pb2.ClientFinish(
                action=action,
                key=response.key,
            )
        )
        if response_finish.state == 1:
            print("System: creation finish")

    def open(self, filename, primary):
        src_path = self.curr_path + filename
        local_path = self.root + src_path

        response = self.main_stub.getDataNode(
            nameNode_pb2.ClientFileRequest(
                path=src_path,
                action=int(primary),
            )
        )
        if response.state == 0:
            print("System: "+filename+" is locking")
            return
        self.openKey = [response.key, primary]

    def close(self, filename):
        self.main_stub.wave(
            nameNode_pb2.ClientFinish(
                action=int(self.openKey[1]),
                key=int(self.openKey[0])
            )
        )


    def downloadFile(self, filename):
        src_path = self.curr_path + filename
        local_path = self.root + src_path

        response = self.main_stub.getDataNode(
            nameNode_pb2.ClientFileRequest(
                path=src_path,
                action=0,
            )
        )

        if response.state == 0:
            print("System: fail to download the file: ", src_path)
            return

        if len(response.nodes) == 0:
            print("System: The file not exist")
            return

        node = response.nodes[-1]
        response.nodes.pop()
        channel = grpc.insecure_channel(node.ip + ':' +node.port)
        stub = dataNode_pb2_grpc.dataNodeStub(channel)
        response_data = stub.read(
            dataNode_pb2.ClientReadRequest(
                key=response.key
            )
        )

        self.saveChunk(response_data, local_path)
        print("System: File downloaded successfully")
        self.main_stub.wave(
            nameNode_pb2.ClientFinish(
                action=0,
                key=response.key
            )
        )

    def deleteFile(self, filename):
        origin_path = self.curr_path + filename
        # local_path = self.root + origin_path
        response = self.main_stub.getDataNode(
            nameNode_pb2.ClientFileRequest(
                path=origin_path,
                action=-1
            )
        )
        if response.state == 1:
            print("System: success to delete the file: ", origin_path)
        else:
            print("System: fail to delete the file: ", origin_path)

    # 文件目录相关操作

    def openDir(self, target_dir):
        if target_dir == '.':
            print("System: Successfully open dir ", target_dir)
            return
        if target_dir == '..':
            if self.curr_path == '/':
                print("System: Successfully open dir ", target_dir)
                return
            path = os.path.normpath(self.curr_path).split(os.sep)
            path.pop()
            self.curr_path = '/'
            for dir in path:
                if dir != '':
                    self.curr_path += dir + '/'
            print("System: Successfully open dir ", target_dir)
            return
        path = os.path.normpath(target_dir).split(os.sep)

        if path[0] == '.':
            path = path[1:]
            target_dir = ''
            for dir in path:
                target_dir += dir + '/'
        if path[0] == '..':
            path = path[1:]
            target_dir = ''
            for dir in path:
                target_dir += dir + '/'
            path = os.path.normpath(self.curr_path).split(os.sep)
            path.pop()
            self.curr_path = '/'
            for dir in path:
                self.curr_path += dir + '/'

        response = self.main_stub.cd(
            nameNode_pb2.ClientCdRequest(
                path=self.curr_path + target_dir,
            )
        )
        if response.state == 0:
            print("System: Error!! " + target_dir + " does not exist")
            return
        print("System: Successfully open dir ", target_dir)
        self.curr_path += target_dir + '/'

    def lsDir(self, sub_dir=""):
        response = self.main_stub.ls(
            nameNode_pb2.ClientLsRequest(
                path=self.curr_path+sub_dir,
            )
        )
        print(response.objects)

    def addDir(self, dir_name):
        response = self.main_stub.mkdir(
            nameNode_pb2.ClientMkdirRequest(
                path=self.curr_path+dir_name
            )
        )
        if response.state == 0:
            print("System: Error in creating new dir ", self.curr_path+dir_name)
            return
        print("System: Successfully create new dir ",  self.curr_path+dir_name)

def run(id):
    root = './clientData/Client_%s/'%(id)
    client = Client(id, root)

    if not os.path.exists(client.root):
        os.makedirs(client.root)

    print("==============================")
    print("Welcome!!! Client", id)
    print("==============================")
    root = '$ '

    while True:
        print(root, end='')
        command = input().split()
        opera = command[0].lower()
        if opera == 'upload':
            filename = command[1]
            client.uploadFile(filename, 2)
        elif opera == 'download':
            filename = command[1]
            client.downloadFile(filename)
        elif opera == 'delete':
            filename = command[1]
            client.deleteFile(filename)
        elif opera == 'cd':
            dir_name = command[1]
            client.openDir(dir_name)
        elif opera == 'ls':
            if len(command) > 1:
                dir_name = command[1]
            else:
                dir_name = ''
            client.lsDir(dir_name)
        elif opera == 'mkdir':
            dir_name = command[1]
            client.addDir(dir_name)
        elif opera == 'open':
            filename = command[1]
            primary = command[2]
            client.open(filename, primary)
        elif opera == 'close':
            filename = command[1]
            client.close(filename)
        elif opera == 'exit':
            break
        else:
            print("System: Error command. Please input the correct command")



if __name__ == '__main__':
    run(1)





