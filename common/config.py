# common/config.py

# NameNode 监听地址
NAMENODE_HOST = '127.0.0.1'
NAMENODE_PORT = 50051

# DataNode 列表（client 与 NameNode 随机调度）
DATANODES = [
('127.0.0.1', 50061),
('127.0.0.1', 50062),
]