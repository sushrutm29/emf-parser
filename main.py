from mfstruct import create_struct
import psycopg2 as pg2
from pprint import pprint
from prettytable import PrettyTable
import re

groupAttrIndices, fVs_temp, gVs, selects, attrIndex, hav, gA, fVs = create_struct()

# Executing query using created necessary structures
try:
    connection = pg2.connect(user = "postgres",
                            password = "grespost123",
                            host = "127.0.0.1",
                            port = "5432",
                            database = "postgres")
    cursor = connection.cursor()
    select_query = '''SELECT * from sales'''
    cursor.execute(select_query)

    # Creating result table schema
    queryResult = PrettyTable()
    field_names = []

    for i in range(len(selects)):
        field_names.append(selects[i])
    
    queryResult.field_names = field_names

    # This is the object/dict which stores all grouping attributes and their corresponding aggregate function results
    groups = {}

    # Iterating through each row
    for i in range(cursor.rowcount):
        row = cursor.fetchone()
        groupTuple = tuple()
        currGVs = []

        for gV in gVs.keys():
            currGVs.append(gV)

        #  Forms a tuple of grouping attributes to store in groups object
        for j in range(7):
            if j in groupAttrIndices:
                groupTuple += (row[j],)
        
        # This loop identifies which, if any, grouping variable condition is satisfied by current row
        for gV, conds in gVs.items():
            for attr, index in attrIndex.items():
                conds = conds.replace(attr, "row["+str(index)+"]")
            if not eval(conds):
                currGVs.remove(gV)
            if len(currGVs) == 0:
                break
        
        if groupTuple not in groups:
            groups[groupTuple] = {}

            for j in range(len(fVs_temp)):
                groups[groupTuple][fVs_temp[j]['gV']+"_"+fVs_temp[j]['aggr']+"_"+fVs_temp[j]['attr']] = 0

        # Skip current row, if it does not satisfy any grouping variable condition
        if len(currGVs) == 0:
            continue
        
        # Compute or update f-vect values upto current row
        for j in range(len(fVs_temp)):
            if fVs_temp[j]['gV'] in currGVs:
                for k in range(7):
                    if attrIndex[fVs_temp[j]['attr']] == k:
                        if fVs_temp[j]['aggr'] == 'sum':
                            if fVs_temp[j]['gV']+"_sum_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                groups[groupTuple][fVs_temp[j]['gV']+"_sum_"+fVs_temp[j]['attr']] = row[k]
                            else:
                                groups[groupTuple][fVs_temp[j]['gV']+"_sum_"+fVs_temp[j]['attr']] += row[k]
                        if fVs_temp[j]['aggr'] == 'count':
                            if fVs_temp[j]['gV']+"_count_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                groups[groupTuple][fVs_temp[j]['gV']+"_count_"+fVs_temp[j]['attr']] = 1
                            else:
                                groups[groupTuple][fVs_temp[j]['gV']+"_count_"+fVs_temp[j]['attr']] += 1
                        if fVs_temp[j]['aggr'] == 'max':
                            if fVs_temp[j]['gV']+"_max_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                groups[groupTuple][fVs_temp[j]['gV']+"_max_"+fVs_temp[j]['attr']] = row[k]
                            else:
                                if groups[groupTuple][fVs_temp[j]['gV']+"_max_"+fVs_temp[j]['attr']] < row[k]:
                                    groups[groupTuple][fVs_temp[j]['gV']+"_max_"+fVs_temp[j]['attr']] = row[k]
                        if fVs_temp[j]['aggr'] == 'min':
                            if fVs_temp[j]['gV']+"_min_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                groups[groupTuple][fVs_temp[j]['gV']+"_min_"+fVs_temp[j]['attr']] = row[k]
                            else:
                                if groups[groupTuple][fVs_temp[j]['gV']+"_min_"+fVs_temp[j]['attr']] > row[k]:
                                    groups[groupTuple][fVs_temp[j]['gV']+"_min_"+fVs_temp[j]['attr']] = row[k]
                        if fVs_temp[j]['aggr'] == 'avg':
                            if fVs_temp[j]['gV']+"_avg_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                if fVs_temp[j]['gV']+"_sum_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                    continue
                                if fVs_temp[j]['gV']+"_count_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                    continue
                            groups[groupTuple][fVs_temp[j]['gV']+"_avg_"+fVs_temp[j]['attr']] = groups[groupTuple][fVs_temp[j]['gV']+"_sum_"+fVs_temp[j]['attr']] / groups[groupTuple][fVs_temp[j]['gV']+"_count_"+fVs_temp[j]['attr']]

    # Modifying having clause to be easier to evaluate by replacing aggregates with their values
    flag = False

    for gTuple, aggrSet in groups.items():
        currHaving = hav

        for aggrName, aggrValue in aggrSet.items():
            currHaving = currHaving.replace(aggrName, str(aggrValue))

        # Replacing every individual "=" with "==" in having string
        currHaving = re.sub(r"[^<>!]=", "==", currHaving)
    
        # Add to query result table if row satisfies having condition
        if eval(currHaving):
            rowToAdd = []

            for i in range(len(selects)):
                if selects[i] in gA:
                    for j in range(len(gA)):
                        if selects[i] == gA[j]:
                            rowToAdd.append(gTuple[j])
                else:
                    rowToAdd.append(aggrSet[selects[i]])

            queryResult.add_row(rowToAdd)
        
    print(queryResult)
        
except (Exception, pg2.DatabaseError) as error:
    print("Error executing query: ", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("\nQuery executed and connection closed successfully!\n")