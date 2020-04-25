import psycopg2 as pg2
import json
from pprint import pprint
from prettytable import PrettyTable as Table
from readDb import readDb
from readInput import readInput

mobile_records = readDb()
select, ngv, gA, fV, cond, hav = readInput()

attrIndex = {
    "cust": 0,
    "prod": 1,
    "day": 2,
    "monthh": 3,
    "year": 4,
    "state": 5,
    "quant": 6
}
groupAttrIndices = set()

for i in range(0, len(gA)):
    groupAttrIndices.add(attrIndex[gA[i]])

groups = {}

for row in mobile_records:
    groupTuple = tuple()

    for i in range(7):
        if i in groupAttrIndices:
            groupTuple += (row[i],)

    if groupTuple not in groups:
        groups[groupTuple] = []
    groups[groupTuple].append({"cust": row[0], "prod": row[1], "day": row[2], "month": row[3], "year": row[4], "state": row[5], "quant": row[6]})

pprint(groups)