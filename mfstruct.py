import json
from readInput import readInput
import copy

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
        "monthh": 3,
        "year": 4,
        "state": 5,
        "quant": 6
    }

    gVs = {"1": [], "2": [], "3": []}

    # Organizing grouping variables and their conditions into dictionaries
    for i in range(1,ngv+1):
        for c in cond:
            if int(c[0]) == i:
                gVs[str(i)].append(c[2:])

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

    # Organizing grouping variable conditions into their attribute to be compared, comparison operator and value
    for gV, gVconds in gVs.items():
        for j in range(len(gVconds)):
            operator = ""
            attribute = ""
            value = ""
            attrEnd = False
            for i in range(len(gVconds[j])):
                if gVconds[j][i] in {"=", ">", "<"}:
                    attrEnd = True
                    if gVconds[j][i] == "=":
                        operator += "=="
                    else: 
                        operator += gVconds[j][i]
                elif attrEnd:
                    value += gVconds[j][i]
                if not attrEnd:
                    attribute += gVconds[j][i]

            gVconds[j] = {"attribute": attribute, "operator": operator, "value": value}

    return groupAttrIndices, fVs_temp, gVs, selects, attrIndex, hav

