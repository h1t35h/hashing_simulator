import hashlib
from typing import List, Any
from bisect import bisect_left

from server import Server


def get_hash_key(key: Any) -> int:
    return int(hashlib.sha256(str(key).encode('utf-8')).hexdigest(), 16) % 10 ** 8


def modulo_hashing(num_servers: int, key: int):
    hash_key = get_hash_key(key)
    return hash_key % num_servers


def consistent_hashing(servers: List[Server], num_replica: int = 1):
    server_mappings = []
    for index, server in enumerate(servers):
        for replica in range(num_replica):
            virtual_node_name = f'{server.server_name}-replica-{replica}'
            virtual_node_hash = get_hash_key(virtual_node_name)
            server_mappings.append({
                'hash': virtual_node_hash,
                'index': index,
                'server_id': server.server_name,
                'vnode_id': virtual_node_name})

    server_mappings.sort(key=lambda val: val['hash'])
    return server_mappings


def get_server_consistent_hash(server_keys: List, key: int) -> int:
    hash_key = get_hash_key(key)
    index = bisect_left(server_keys, hash_key)
    print(f'hash_key: {hash_key} index: {index}, server_keys: [{server_keys}]')
    if index == len(server_keys):
        return 0
    else:
        return index
