# datanode/server.py

import os
from concurrent import futures

import grpc
import hdfs_pb2, hdfs_pb2_grpc
from common.config import NAMENODE_HOST, NAMENODE_PORT, DATANODES

# 从环境变量读取自己监听的端口
import sys
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else DATANODES[0][1]
BLOCK_DIR = 'datanode/blocks'
os.makedirs(BLOCK_DIR, exist_ok=True)

class DataNodeServicer(hdfs_pb2_grpc.DataNodeServicer):
    def WriteBlock(self, request, context):
        path = os.path.join(BLOCK_DIR, request.block_id)
        with open(path, 'wb') as f:
            f.write(request.data)
        return hdfs_pb2.Ack(status='OK')

    def ReadBlock(self, request, context):
        path = os.path.join(BLOCK_DIR, request.block_id)
        if not os.path.exists(path):
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details('Block not found')
            return hdfs_pb2.BlockData()
        data = open(path, 'rb').read()
        return hdfs_pb2.BlockData(block_id=request.block_id, data=data)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    hdfs_pb2_grpc.add_DataNodeServicer_to_server(DataNodeServicer(), server)
    server.add_insecure_port(f'[::]:{PORT}')
    print(f"DataNode started at port {PORT}, storing blocks in {BLOCK_DIR}")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()