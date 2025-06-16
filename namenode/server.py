# namenode/server.p
import os
import json
import random
from concurrent import futures

import grpc
import hdfs_pb2, hdfs_pb2_grpc
from common.config import DATANODES, NAMENODE_HOST, NAMENODE_PORT
METADATA_FILE = 'namenode/metadata.json'
BLOCK_REPLICA = 2 # 每个块副本数

        # 初始化元数据存储
if not os.path.exists(METADATA_FILE):
    with open(METADATA_FILE, 'w') as f:
        json.dump({}, f)

class NameNodeServicer(hdfs_pb2_grpc.NameNodeServicer):
    def _load_meta(self):
        with open(METADATA_FILE, 'r') as f:
            return json.load(f)

    def _save_meta(self, meta):
        with open(METADATA_FILE, 'w') as f:
            json.dump(meta, f, indent=2)

    def CreateFile(self, request, context):
        filename = request.filename
        meta = self._load_meta()
        if filename in meta:
            # 已存在则返回已有信息
            locs = meta[filename]
        else:
             # 切块策略：这里不实际切，只生成一个块 ID
             block_id = f"{filename}_blk_{random.randint(1000,9999)}"
             # 随机选 BLOCK_REPLICA 个 DataNode
             replicas = random.sample(DATANODES, BLOCK_REPLICA)
             locs = {
             'block_ids': [block_id],
             'datanodes': [f"{h}:{p}" for h, p in replicas]
             }
             meta[filename] = locs
             self._save_meta(meta)
        return hdfs_pb2.BlockLocations(
            block_ids=locs['block_ids'],
            datanodes=locs['datanodes']
        )

    def GetFile(self, request, context):
        meta = self._load_meta()
        locs = meta.get(request.filename, {'block_ids': [], 'datanodes': []})
        return hdfs_pb2.BlockLocations(
            block_ids=locs['block_ids'],
            datanodes=locs['datanodes']
        )

    def ListFiles(self, request, context):
        meta = self._load_meta()
        return hdfs_pb2.FileList(filenames=list(meta.keys()))


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    hdfs_pb2_grpc.add_NameNodeServicer_to_server(NameNodeServicer(), server)
    server.add_insecure_port(f'{NAMENODE_HOST}:{NAMENODE_PORT}')
    print(f"NameNode started at {NAMENODE_HOST}:{NAMENODE_PORT}")
    server.start()
    server.wait_for_termination()
if __name__ == '__main__':
    serve()