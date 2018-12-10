from gevent import socket


class Address:
    def __init__(self, address):
        if address.startswith('unix:'):
            self.family = socket.AF_UNIX
            self.address = address[7:]
        else:
            self.family = socket.AF_INET
            ip, _, port = address.partition(':')
            self.address = ip, int(port)
