import gevent
from gevent import socket

from bsonrpc import JSONRpc, request, service_class, ThreadingModel

from conf import settings
from utils.collections import MultiVersionSet


@service_class
class LogServer:
    protocol = 0

    def __init__(self, engine):
        self.engine = engine
        self.field_indexes = None
        self.where = None

    @request
    def connect(self, query='SELECT *', protocol=None):
        if protocol != self.protocol:
            return {'error': 'unknown protocol'}
        fields = self.engine.fields[:-2]
        self.field_indexes = tuple(self.engine.fields.index(field) for field in fields)
        self.where = self.effective
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

    @request
    def commit(self, add, remove):
        # TODO: maybe we need a `ts` and a `read` argument?
        # TODO: what's the difference between this and postgres' approach, which blocks some writes?
        # check remove conflicts
        for row in remove:
            if self.engine.rows

    def effective(self, row):
        return row[-1] is None


class Log:
    def __init__(self, name, fields):
        self.name = name
        self.address = str(settings.SOCKET_DIR / name)

        self.fields = (*fields, '@add', '@remove')
        self.rows = MultiVersionSet()
        self.data = {}

        # listen for incoming connections
        s = socket.socket(socket.AF_UNIX)
        s.bind(self.address)
        s.listen()

        def server():
            while True:
                JSONRpc(s.accept()[0],
                        LogServer(self),
                        threading_model=ThreadingModel.GEVENT,
                        concurrent_request_handling=ThreadingModel.GEVENT)
        gevent.spawn(server)
