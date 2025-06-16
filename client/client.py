# client/client.py
import os
import sys
import grpc
import hdfs_pb2, hdfs_pb2_grpc
from common.config import NAMENODE_HOST, NAMENODE_PORT

def upload(filename):
    # 1. 请求创建文件，获取块 ID 和 DataNode 地址
    channel = grpc.insecure_channel(f'{NAMENODE_HOST}:{NAMENODE_PORT}')
    stub = hdfs_pb2_grpc.NameNodeStub(channel)
    locs = stub.CreateFile(hdfs_pb2.FileRequest(filename=filename))
    block_id = locs.block_ids[0]
    # 2. 读取文件，发送给第一个 DataNode
    data = open(filename, 'rb').read()
    dn_addr = locs.datanodes[0]
    ch_dn = grpc.insecure_channel(dn_addr)
    dn_stub = hdfs_pb2_grpc.DataNodeStub(ch_dn)
    resp = dn_stub.WriteBlock(hdfs_pb2.BlockData(block_id=block_id, data=data))
    print(f"Uploaded block {block_id} to {dn_addr}, status: {resp.status}")
def download(filename):
    # 1. 请求获取块列表
    channel = grpc.insecure_channel(f'{NAMENODE_HOST}:{NAMENODE_PORT}')
    stub = hdfs_pb2_grpc.NameNodeStub(channel)
    locs = stub.GetFile(hdfs_pb2.FileRequest(filename=filename))
    if not locs.block_ids:
        print("文件不存在")
        return
    # 2. 从第一个 DataNode 读取
    block_id = locs.block_ids[0]
    dn_addr = locs.datanodes[0]
    ch_dn = grpc.insecure_channel(dn_addr)
    dn_stub = hdfs_pb2_grpc.DataNodeStub(ch_dn)
    blk = dn_stub.ReadBlock(hdfs_pb2.BlockID(block_id=block_id))
    out = f"downloaded_{filename}"
    open(out, 'wb').write(blk.data)
    print(f"Downloaded to {out}")
def list_files():
    channel = grpc.insecure_channel(f'{NAMENODE_HOST}:{NAMENODE_PORT}')
    stub = hdfs_pb2_grpc.NameNodeStub(channel)
    fl = stub.ListFiles(hdfs_pb2.Empty())
    print("Files in DFS:")
    for fn in fl.filenames:
        print(" -", fn)
def main():
    if len(sys.argv) < 2:
        print("Usage: python client.py [upload|download|list] [filename]")
        return
    cmd = sys.argv[1]
    if cmd == 'upload' and len(sys.argv)==3:
        upload(sys.argv[2])
    elif cmd == 'download' and len(sys.argv)==3:
        download(sys.argv[2])
    elif cmd == 'list':
        list_files()
    else:
        print("Invalid command")
if __name__ == '__main__':
    main()