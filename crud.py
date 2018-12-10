from pathlib import Path
import socket
from time import time

from bsonrpc import JSONRpc

SOCKET_ADDR = Path('/tmp/reactdb/test')


def main():
    conn = socket.socket(socket.AF_UNIX)
    conn.connect(str(SOCKET_ADDR))
    table = JSONRpc(conn).get_peer_proxy()

    response = table.connect(where='salary>5000 OR name="Mike"')
    fields = response['fields']
    version = response['version']

    while True:
        print('#', *fields, sep='\t')
        response = table.query(withdata=True)
        rows = set(response['rows'])
        data = response['data']
        for row in rows:
            print(row, *data[row], sep='\t')
        print()
        method = input('c/u/d > ')

        if method == 'c':
            data = {
                'id': int(input('    id > ')),
                'name': input('    name > '),
                'salary': int(input('    salary > '))
            }
            table.modify(data=data, version=time())
        elif method == 'u':
            row = input('    # > ')
            data = {
                'id': int(input('    id > ')),
                'name': input('    name > '),
                'salary': int(input('    salary > '))
            }
            table.modify(row=row, data=data, version=time())
        elif method == 'd':
            row = input('    # > ')
            table.modify(row=row, version=time())
        else:
            print('error')
        print()


if __name__ == '__main__':
    main()
