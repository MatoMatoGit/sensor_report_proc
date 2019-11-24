import socket
import json


class GraphQLSocket:

    CONFIG_FILE = "/graphql_sock_cfg.json"

    def __init__(self, cfg_dir):
        f_cfg = open(cfg_dir + self.CONFIG_FILE, "r")
        self.Config = json.dump(f_cfg)
        f_cfg.close()

        addr = socket.getaddrinfo(self.Config["ip"], self.Config["port"],
                                  proto=socket.IPPROTO_TCP)[0][-1]
        print(addr)
        self.Socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.Socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.Socket.settimeout(10)
        self.Socket.bind(addr + "/graphql")

        return

    def Send(self, data):
        self.Socket.write(data)

    def Receive(self):
        self.Socket.read()

