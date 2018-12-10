from gevent import event, socket
from pathlib import Path

from bsonrpc import JSONRpc, request, service_class, ThreadingModel

SOCKET_ADDR = Path('/tmp/reactdb/test')


class Relation:
    fields = None
    data = None
    versions = None
    next_row = None


class Version:

    def __init__(self, key, data):
        self.key = key
        self.data = data
        self.event = event.Event()

    def update(self):
        self.event.set()

    def wait(self):
        self.event.wait()

    @staticmethod
    def get(versions, key):
        for version in reversed(versions):
            if version.key <= key:
                return version


relation = Relation()
relation.fields = ('id', 'name', 'salary')
relation.data = {
    '1': (101, 'Mike', 2000),
    '2': (201, 'Bob', 5000),
    '3': (202, 'Alice', 6000),
}
relation.versions = [
    Version(0, set(relation.data.keys())),
]
relation.next_row = 4


@service_class
class RelationServer:

    def __init__(self):
        self.relation = relation
        self.fields = self.relation.fields
        self.where = ''
        self.version = self.relation.versions[-1]

    @request
    def connect(self, fields=None, where=None, version=None):
        if fields is not None:
            self.fields = fields
        if where is not None:
            self.where = where
        if version is not None:
            self.version = Version.get(self.relation.versions, version)
        return {
            'fields': self.fields,
            'version': self.version.key,
        }

    def check(self, row):
        data = self.relation.data[row]
        return data[2] > 5000 or data[1] == 'Mike'

    def checks(self, rows):
        return list(filter(self.check, rows))

    @request
    def query(self, withdata=False):
        response = {'rows': self.checks(self.version.data)}
        if withdata:
            response['data'] = {row: self.relation.data[row] for row in response['rows']}
        return response

    @request
    def wait(self, withdata=False):
        while True:
            self.version.wait()
            target = self.relation.versions[-1]
            insert = self.checks(target.data - self.version.data)
            remove = self.checks(self.version.data - target.data)
            self.version = target
            if insert or remove:
                break
        response = {
            'insert': insert,
            'remove': remove,
        }
        if withdata:
            response['data'] = {row: self.relation.data[row] for row in (insert + remove)}
        return response

    @request
    def modify(self, row=None, data=None, version=None):
        if version is None:
            version = self.version.key
        v = Version.get(self.relation.versions, version)
        assert v is self.relation.versions[-1]
        if v.key != version:
            v = Version(version, set(v.data))
            self.relation.versions[-1].update()
            self.relation.versions.append(v)
        self.version = v
        if data is None:
            self.version.data.discard(row)
        else:
            if row is None:
                default = (None, None, None)
            else:
                default = self.relation.data[row]
                self.version.data.discard(row)
            row = str(self.relation.next_row)
            self.relation.next_row += 1
            self.relation.data[row] = (
                data.get('id', default[0]),
                data.get('name', default[1]),
                data.get('salary', default[2]),
            )
            self.version.data.add(row)
        return {'version': self.version.key}


def main():
    if SOCKET_ADDR.exists():
        SOCKET_ADDR.unlink()
    s = socket.socket(socket.AF_UNIX)
    s.bind(str(SOCKET_ADDR))
    s.listen()
    while True:
        JSONRpc(s.accept()[0],
                RelationServer(),
                threading_model=ThreadingModel.GEVENT,
                concurrent_request_handling=ThreadingModel.GEVENT)


if __name__ == '__main__':
    main()
