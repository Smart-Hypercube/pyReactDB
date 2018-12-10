from pathlib import Path
import socket

from bsonrpc import JSONRpc

SOCKET_ADDR = Path('/tmp/reactdb/test')


def main():
    conn = socket.socket(socket.AF_UNIX)
    conn.connect(str(SOCKET_ADDR))
    table = JSONRpc(conn).get_peer_proxy()

    response = table.connect(where='salary>5000 OR name="Mike"')
    fields = response['fields']
    version = response['version']
    print(*fields, sep='\t')

    response = table.query(withdata=True)
    rows = set(response['rows'])
    data = response['data']
    for row in rows:
        print(*data[row], sep='\t')
    print()

    print('', *fields, sep='\t')
    while True:
        response = table.wait(withdata=True)
        insert = response['insert']
        remove = response['remove']
        data.update(response['data'])
        for row in insert:
            print('+', *data[row], sep='\t')
            rows.add(row)
        for row in remove:
            print('-', *data[row], sep='\t')
            rows.remove(row)


if __name__ == '__main__':
    main()
