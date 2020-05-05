import json
from readInput import readInput
import copy
import re

def __eq__(self, other) : 
    return self.__dict__ == other.__dict__

def create_struct():
    #Reading query input
    selects, ngv, gA, fVs, cond, hav = readInput()
    fVs_temp = copy.deepcopy(fVs)

    # Assigning default index numbers to attribute names
    attrIndex = {
        "cust": 0,
        "prod": 1,
        "day": 2,
        "month": 3,
        "year": 4,
        "state": 5,
        "quant": 6
    }

    gVs = {}

    # Organizing grouping variables and their conditions into dictionaries
    for i in range(1,ngv+1):
        gVs[str(i)] = ""
        for c in cond:
            if int(c[0]) == i:
                j = 2
                while j < len(c):
                    if not c[j] == str(i):
                        gVs[str(i)] += c[j]
                        j+=1
                    else:
                        j+=2

                gVs[str(i)] = re.sub(r"[^<>!]=", "==", gVs[str(i)])

    # Creating a set of grouping attribute indices
    groupAttrIndices = set()

    for i in range(0, len(gA)):
        groupAttrIndices.add(attrIndex[gA[i]])


    # Better organizing F-vect input into grouping variable, aggregate function and attribute
    for i in range(len(fVs_temp)):
        fVs_temp[i] = fVs_temp[i].split('_')
        fVs_temp[i] = {
            "gV": fVs_temp[i][0],
            "aggr": fVs_temp[i][1],
            "attr": fVs_temp[i][2]
        }

    toInsert = []
    toDelete = []

    # Adding sum and count aggregates for every grouping variable and corresponding attribute that has an average aggregate
    for i in range(len(fVs_temp)):
        if fVs_temp[i]['aggr'] == 'avg':
            for j in range(len(fVs_temp)):
                if fVs_temp[j]['gV'] == fVs_temp[i]['gV'] and fVs_temp[j]['attr'] == fVs_temp[i]['attr']:
                    if fVs_temp[j]['aggr'] == 'count' or fVs_temp[j]['aggr'] == 'sum':
                        toDelete.append(fVs_temp[j])

            toInsert.append({
                "gV": fVs_temp[i]['gV'],
                "aggr": 'count',
                "attr": fVs_temp[i]['attr']
            })

            toInsert.append({
                "gV": fVs_temp[i]['gV'],
                "aggr": 'sum',
                "attr": fVs_temp[i]['attr']
            })

    for i in range(len(toDelete)):
        fVs_temp.remove(toDelete[i])

    fVs_temp = toInsert + fVs_temp        

    return groupAttrIndices, fVs_temp, gVs, selects, attrIndex, hav, gA, fVs

