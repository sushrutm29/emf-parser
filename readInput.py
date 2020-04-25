import json

def readInput():
    try:
        with open('input.json') as f:
            inputArgs = json.load(f)

    except (Exception) as error:
        print("Error getting input: ", error)

    finally:
        select = inputArgs['selectAttributes']
        ngv = inputArgs['noOfGroupingVariables']
        gA = inputArgs['groupingAttributes']
        fV = inputArgs['fVector']
        cond = inputArgs['selectConditions']
        hav = inputArgs['havingConditions']

        return select, ngv, gA, fV, cond, hav