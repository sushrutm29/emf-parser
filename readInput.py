import json

def readInput():
    try:
        inputArgs = ""
        with open('input.json') as f:
            inputArgs = json.load(f)

    except (Exception) as error:
        print("Error getting input: ", error)

    finally:
        selects = inputArgs['selectAttributes']
        ngv = inputArgs['noOfGroupingVariables']
        gA = inputArgs['groupingAttributes']
        fVs = inputArgs['fVector']
        cond = inputArgs['selectConditions']
        hav = inputArgs['havingConditions']

        return selects, ngv, gA, fVs, cond, hav