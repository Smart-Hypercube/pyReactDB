"""
CREATE TABLE Department (
    id          integer,
    name        text,
    manager     integer,
);
CREATE TABLE Employee (
    id          integer,
    name        text,
    department  integer,
    salary      integer,
);
CREATE VIEW __join_1 AS
    SELECT l.id, l.name, r.name manager_name
    FROM Department l
    LEFT JOIN Employee r ON l.manager=r.id;
CREATE VIEW __aggregate_1 AS
    SELECT department, SUM(salary) total_salary
    FROM Employee
    GROUP BY department;
CREATE VIEW DepartmentView AS
    SELECT l.id, l.name, l.manager_name, r.total_salary
    FROM __join_1 l
    LEFT JOIN __aggregate_1 r ON l.id=r.department;
"""

from engine import Aggregate, LeftJoin, Log, Table
from engine.aggregate import Sum
from fields import IntegerField, TextField


Department__log = Log('Department__log', {
    'id': IntegerField(),
    'name': TextField(),
    'manager': IntegerField(),
})

Department = Table('Department', Department__log)

Employee_log = Log('Employee_log', {
    'id': IntegerField(),
    'name': TextField(),
    'department': IntegerField(),
    'salary': IntegerField(),
})

Employee = Table('Employee', Employee_log)

__join_1 = LeftJoin('__join_1', Department, 'manager', Employee, 'id', {
    'id': 'id',
    'name': 'name'}, {
    'manager_name': 'name',
})

__aggregate_1 = Aggregate('__aggregate_1', Employee, 'department', {
    'department': 'department',
    'total_salary': Sum('salary'),
})

DepartmentView = LeftJoin('DepartmentView', __join_1, 'id', __aggregate_1, 'department', {
    'id': 'id',
    'name': 'name',
    'manager_name': 'manager_name'}, {
    'total_salary': 'total_salary',
})
