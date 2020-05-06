from mfstruct import create_struct
import psycopg2 as pg2
from pprint import pprint
from prettytable import PrettyTable
import logging

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

    full_table_aggrs = False

    # Check if there are any aggregates on entire group
    for i in range(len(fVs_temp)):
        if fVs_temp[i]['gV'] == "0":
            full_table_aggrs = True
            break
    
    # If there are full group aggregates, compute them first
    if full_table_aggrs:
        for i in range(cursor.rowcount):
            row = cursor.fetchone()
            groupTuple = tuple()

            # Forms a tuple of grouping attributes to store in groups object
            for j in range(7):
                if j in groupAttrIndices:
                    groupTuple += (row[j],)
            
            # Initialize group tuple values if not already done so
            if groupTuple not in groups:
                groups[groupTuple] = {}

                for j in range(len(fVs_temp)):
                    if not fVs_temp[j]['gV'] == "0":
                        groups[groupTuple][fVs_temp[j]['gV']+"_"+fVs_temp[j]['aggr']+"_"+fVs_temp[j]['attr']] = 0
                    else:
                        groups[groupTuple][fVs_temp[j]['aggr']+"_"+fVs_temp[j]['attr']] = 0  

            # Compute or update f-vect values upto current row
            for j in range(len(fVs_temp)):
                for k in range(7):
                    if fVs_temp[j]['gV'] == "0":
                        if attrIndex[fVs_temp[j]['attr']] == k:
                            if fVs_temp[j]['aggr'] == 'sum':
                                if "sum_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                    groups[groupTuple]["sum_"+fVs_temp[j]['attr']] = row[k]
                                else:
                                    groups[groupTuple]["sum_"+fVs_temp[j]['attr']] += row[k]
                            if fVs_temp[j]['aggr'] == 'count':
                                if "count_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                    groups[groupTuple]["count_"+fVs_temp[j]['attr']] = 1
                                else:
                                    groups[groupTuple]["count_"+fVs_temp[j]['attr']] += 1
                            if fVs_temp[j]['aggr'] == 'max':
                                if "max_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                    groups[groupTuple]["max_"+fVs_temp[j]['attr']] = row[k]
                                else:
                                    if groups[groupTuple]["max_"+fVs_temp[j]['attr']] < row[k]:
                                        groups[groupTuple]["max_"+fVs_temp[j]['attr']] = row[k]
                            if fVs_temp[j]['aggr'] == 'min':
                                if "min_"+fVs_temp[j]['attr'] not in groups[groupTuple] or groups[groupTuple]["min_"+fVs_temp[j]['attr']] == 0:
                                    groups[groupTuple]["min_"+fVs_temp[j]['attr']] = row[k]
                                else:
                                    if groups[groupTuple]["min_"+fVs_temp[j]['attr']] > row[k]:
                                        groups[groupTuple]["min_"+fVs_temp[j]['attr']] = row[k]
                            if fVs_temp[j]['aggr'] == 'avg':
                                if "avg_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                    if "sum_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                        continue
                                    if "count_"+fVs_temp[j]['attr'] not in groups[groupTuple]:
                                        continue
                                groups[groupTuple]["avg_"+fVs_temp[j]['attr']] = groups[groupTuple]["sum_"+fVs_temp[j]['attr']] / groups[groupTuple]["count_"+fVs_temp[j]['attr']]                                  

        cursor.execute(select_query)

    # Iterating through each row
    for i in range(cursor.rowcount):
        row = cursor.fetchone()
        groupTuple = tuple()
        currGVs = []

        for gV in gVs.keys():
            currGVs.append(gV)

        # Forms a tuple of grouping attributes to store in groups object
        for j in range(7):
            if j in groupAttrIndices:
                groupTuple += (row[j],)
        
        # Initialize group tuple values if not already done so
        if groupTuple not in groups:
            groups[groupTuple] = {}

            for j in range(len(fVs_temp)):
                if not fVs_temp[j]['gV'] == "0":
                    groups[groupTuple][fVs_temp[j]['gV']+"_"+fVs_temp[j]['aggr']+"_"+fVs_temp[j]['attr']] = 0
                else:
                    groups[groupTuple][fVs_temp[j]['aggr']+"_"+fVs_temp[j]['attr']] = 0

        # This loop identifies which, if any, grouping variable condition is satisfied by current row
        for gV, conds in gVs.items():
            for j in range(len(fVs_temp)):
                if fVs_temp[j]['gV'] == "0":
                    conds = conds.replace(str(fVs_temp[j]['aggr']+"_"+fVs_temp[j]['attr']), str(groups[groupTuple][fVs_temp[j]['aggr']+"_"+fVs_temp[j]['attr']]))
            
            for attr, index in attrIndex.items():
                conds = conds.replace(attr, "row["+str(index)+"]")

            if not eval(conds):
                currGVs.remove(gV)
            if len(currGVs) == 0 and not full_table_aggrs:
                break

        # Skip current row, if it does not satisfy any grouping variable condition and no aggregates for entire group
        if len(currGVs) == 0 and not full_table_aggrs:
            continue
        
        # Compute or update f-vect values upto current row
        for j in range(len(fVs_temp)):
            for k in range(7):
                if fVs_temp[j]['gV'] in currGVs:
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
                            if fVs_temp[j]['gV']+"_min_"+fVs_temp[j]['attr'] not in groups[groupTuple] or groups[groupTuple][fVs_temp[j]['gV']+"_min_"+fVs_temp[j]['attr']] == 0:
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
    for gTuple, aggrSet in groups.items():
        currHaving = hav

        if not currHaving == "":
            for aggrName, aggrValue in aggrSet.items():
                currHaving = currHaving.replace(aggrName, str(aggrValue))

            # Replacing every individual "=" with "==" in having string
            currHaving = currHaving.replace("=", "==")
            currHaving = currHaving.replace(">==", ">=")
            currHaving = currHaving.replace("<==", "<=")
            currHaving = currHaving.replace("!==", "!=")

        # Add to query result table if row satisfies having condition
        if currHaving == "" or eval(currHaving):
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
    print("Error executing query: ", logging.exception(error))

finally:
    if connection:
        cursor.close()
        connection.close()
        print("\nQuery executed and connection closed successfully!\n")