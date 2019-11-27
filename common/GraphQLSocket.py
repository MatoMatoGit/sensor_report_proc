import socket
import json


class GraphQLSocket:

    CONFIG_FILE = "/graphql_sock_cfg.json"
    CONFIG_IP = "ip"
    CONFIG_PORT = "port"

    def __init__(self, cfg_dir):
        f_cfg = open(cfg_dir + self.CONFIG_FILE, "r")
        self.Config = json.dump(f_cfg)

        if self._VerifyConfig() is False:
            return -1

        f_cfg.close()

        addr = socket.getaddrinfo(self.Config[self.CONFIG_IP], self.Config[self.CONFIG_PORT],
                                  proto=socket.IPPROTO_TCP)[0][-1]
        print(addr)
        self.Socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.Socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.Socket.settimeout(10)
        self.Socket.bind(addr + "/graphql")

        return 0

    def _VerifyConfig(self):
        if self.CONFIG_IP not in self.Config:
            return False
        if self.CONFIG_PORT not in self.Config:
            return False

        return True

    def Send(self, data):
        self.Socket.write(data)

    def Receive(self):
        self.Socket.read()

