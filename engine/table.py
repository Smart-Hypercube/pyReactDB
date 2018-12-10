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
        if protocol != self.protocol:
            return {'error': 'unknown protocol'}
        fields = self.engine.fields
        self.field_indexes = tuple(self.engine.fields.index(field) for field in fields)
        self.where = self.true
        return {'fields': fields}

    @request
    def query(self, ts, nonblock=False, data=True):
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

    def true(self, row):
        return True


class Table:
    def __init__(self, name, log):
        self.name = name
        self.address = str(settings.SOCKET_DIR / name)

        # connect to log and retrieve field names
        s = socket.socket(socket.AF_UNIX)
        s.connect(log.address)
        self.log = JSONRpc(s).get_peer_proxy()
        self.fields = self.log.connect('SELECT * WHERE effective()', protocol=0)['fields']
        self.rows = MultiVersionSet()
        self.data = {}

        # get updates from log
        def chase():
            while True:
                response = self.log.query(self.rows.key)
                rows = set(self.rows.value)
                rows |= set(response['add'])
                rows -= set(response['remove'])
                self.rows[response['ts']] = rows
                self.data.update(response['data'])
        gevent.spawn(chase)

        # listen for incoming connections
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
