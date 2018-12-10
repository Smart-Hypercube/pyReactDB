from pathlib import Path
import socket
from time import time

from bsonrpc import JSONRpc

SOCKET_DIR = Path('/tmp/reactdb')


def main():
    name = input('relation > ')
    query = input(name + ' > ')

    conn = socket.socket(socket.AF_UNIX)
    conn.connect(str(SOCKET_DIR / name))
    table = JSONRpc(conn).get_peer_proxy()
    fields = table.connect(query=query, protocol=0)['fields']

    while True:
        print('#', *fields, sep='\t| ')
        print('-' * 80)
        response = table.query()
        rows = response['add']
        data = response['data']
        for row in rows:
            print(row, *data[row], sep='\t| ')
        print()
        method = input('c/u/d > ')

        if method == 'c':
            data = input('c > ').split()
            table.commit(time(), [data], [])
        elif method == 'u':
            row = input('# > ')
            data = input('u > ').split()
            table.commit(time(), [data], [row])
        elif method == 'd':
            row = input('# > ')
            table.commit(time(), [], [row])
        else:
            print('error')
        print()


if __name__ == '__main__':
    main()
