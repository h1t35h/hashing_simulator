# Server
from typing import Dict


class Server:
    server_name: str
    storage: Dict[int, str]

    def __init__(self, server_name: str):
        self.server_name = server_name
        self.storage = {}

    def add_data(self, key: int, value: str):
        self.storage[key] = value

    def get_data(self, key: int):
        if key in self.storage:
            return self.storage[key]
        else:
            return 'No Data Found'

    def print_data(self):
        print(f'ServerName:{self.server_name} Data: {self.storage}')
