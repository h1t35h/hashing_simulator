from typing import List, Dict
from server import Server
from hashing import modulo_hashing, consistent_hashing, get_server_consistent_hash


class Simulator:
    servers: List[Server]
    server_name: int
    simulate_algorithm: str
    server_mappings: List
    server_keys: List
    replicas: int

    def __init__(self, simulate_algorithm: str = 'modulo', replicas: int = 1):
        self.servers = []
        self.server_name = 0
        self.server_mappings = []
        self.server_keys = []
        self.simulate_algorithm = simulate_algorithm
        self.replicas = replicas

    def add_server(self, num_servers: int = 1):
        for i in range(num_servers):
            count = self.server_name
            self.server_name += 1
            self.servers.append(Server(server_name=f'Server-{count}'))
            self.server_mappings = consistent_hashing(self.servers, num_replica=self.replicas)
            self.server_keys = [node['hash'] for node in self.server_mappings]

    def remove_server(self, server_id: int):
        for server in self.servers:
            if server.server_name == f'Server-{server_id}':
                self.servers.remove(server)
        self.server_mappings = consistent_hashing(self.servers, num_replica=self.replicas)
        self.server_keys = [node['hash'] for node in self.server_mappings]

    def store_key(self, key: int, value: str):
        server_id = self.__get_server_id(key=key)
        self.servers[server_id].add_data(key=key, value=value)

    def get_key(self, key: int) -> str:
        server_id = self.__get_server_id(key=key)
        return self.servers[server_id].get_data(key=key)

    def print_state(self):
        print(f'server_mappings: {self.server_mappings}')
        print(f'server_keys: {self.server_keys}')
        for server in self.servers:
            server.print_data()

    def __get_server_id(self, key: int):
        if self.simulate_algorithm == 'modulo':
            return modulo_hashing(num_servers=len(self.servers), key=key)
        if self.simulate_algorithm == 'consistent':
            server_hash = get_server_consistent_hash(server_keys=self.server_keys, key=key)
            server_idx = self.server_mappings[server_hash]['index']
            return server_idx
