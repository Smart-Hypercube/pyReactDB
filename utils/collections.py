from gevent.event import Event


class MultiVersionSet:
    def __init__(self):
        self.versions = []
        self.data = {}
        self.events = {}

    def __matmul__(self, k):
        if k in self.data:
            return k
        for key in reversed(self.versions):
            if key <= k:
                return key
        raise KeyError('version key lower than any version')

    def __getitem__(self, k):
        return self.data[self @ k]

    def __setitem__(self, key, value):
        if key <= self.key:
            raise KeyError('version key lower than current version')
        self.events[self.key].set()
        self.versions.append(key)
        self.data[key] = frozenset(value)
        self.events[key] = Event()

    @property
    def key(self):
        return self.versions[-1]

    @property
    def value(self):
        return self.data[self.versions[-1]]

    def wait(self, k=None):
        if k is None:
            key = self.key
        else:
            key = self @ k
        self.events[key].wait()
