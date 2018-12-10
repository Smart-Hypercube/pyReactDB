from pathlib import Path

from engine import AutoField, IntegerField, TextField
from engine.log import LogEngine

address = Path('/tmp/reactdb/department__log')
fields = {
    'id': AutoField(),
    'name': TextField(),
    'manager': IntegerField(),
}

if __name__ == '__main__':
    if address.exists():
        address.unlink()
    LogEngine(
        address='unix://' + str(address),
        fields=fields,
    ).run()
