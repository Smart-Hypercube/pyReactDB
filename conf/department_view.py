from pathlib import Path

from engine.join import run_left

address = Path('/tmp/reactdb/department_view')
fields = {
    'id': AutoField(),
    'name': TextField(),
    'manager': IntegerField(),
}

if __name__ == '__main__':
    if address.exists():
        address.unlink()
    run_left(
        address='unix://' + str(address),
        left='unix:///tmp/reactdb/__join_1',
        right='unix:///tmp/reactdb/__join',
        on=('id', 'department'),
    )
