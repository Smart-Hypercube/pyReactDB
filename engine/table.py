import gevent
from gevent import socket
from itertools import chain

from bsonrpc import JSONRpc, request, service_class, ThreadingModel

from conf import settings
from utils.collections import MultiVersionSet


@service_class
class TableServer:
    protocol = 0

    def __init__(self, engine):
        self.engine = engine
        self.field_indexes = None
        self.where = None

    @request
    def connect(self, query='SELECT *', protocol=None):
        print('connect', query, protocol)
        if protocol != self.protocol:
            return {'error': 'unknown protocol'}
        fields = self.engine.fields
        self.field_indexes = tuple(self.engine.fields.index(field) for field in fields)
        self.where = self.true
        return {'fields': fields}

    @request
    def query(self, ts=0, nonblock=True, data=True):
        print('query', ts, nonblock, data)
        rows = self.engine.rows
        while True:
            if not nonblock:
                rows.wait(ts)
            new = rows.key
            add = list(filter(self.where, rows[new] - rows[ts]))
            remove = list(filter(self.where, rows[ts] - rows[new]))
            if add or remove or nonblock:
                break
        response = {
            'ts': new,
            'add': add,
            'remove': remove,
        }
        if data:
            d = {}
            for row in chain(add, remove):
                row_data = self.engine.data[row]
                d[row] = tuple(row_data[index] for index in self.field_indexes)
            response['data'] = d
        return response

    @request
    def commit(self, ts, add, remove):
        print('commit', ts, add, remove)
        rows = set(self.engine.rows.value)
        for row_data in add:
            self.engine.row_n += 1
            row = str(self.engine.row_n)
            try:
                self.engine.data[row] = tuple(self.engine.field_classes[field](row_data[i])
                                              for (i, field) in enumerate(self.engine.fields))
            except ValueError:
                return {'error': 'invalid input in: ' + repr(row_data)}
            rows.add(row)
        for row in remove:
            try:
                rows.remove(row)
            except KeyError:
                return {'error': 'invalid row to remove: ' + repr(row)}
        self.engine.rows[ts] = rows
        return {}

    def true(self, row):
        return True


class Table:
    def __init__(self, name, fields):
        self.name = name

        self.fields = tuple(fields.keys())
        self.field_classes = fields
        self.rows = MultiVersionSet()
        self.data = {}
        self.row_n = 0  # most recently used row number

        # listen for incoming connections
        address = settings.SOCKET_DIR / name
        if address.exists():
            address.unlink()
        self.address = str(address)
        s = socket.socket(socket.AF_UNIX)
        s.bind(self.address)
        s.listen()

        def server():
            while True:
                JSONRpc(s.accept()[0],
                        TableServer(self),
                        threading_model=ThreadingModel.GEVENT,
                        concurrent_request_handling=ThreadingModel.GEVENT)
        gevent.spawn(server)
