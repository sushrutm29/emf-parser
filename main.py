from mfstruct import create_struct
import psycopg2 as pg2
from pprint import pprint

groupAttrIndices, fVs_temp, gVs, selects, attrIndex, hav = create_struct()

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

    # This is the object/dict which stores all grouping attributes and their corresponding aggregate function results
    groups = {}

    # Iterating through each row
    for i in range(cursor.rowcount):
        row = cursor.fetchone()
        groupTuple = tuple()
        currGVs = []

        for gV in gVs.keys():
            currGVs.append(gV)

        # This loop identifies which, if any, grouping variable condition is satisfied by current row
        #  Also forms a tuple of grouping attributes to store in groups object
        for j in range(7):
            if j in groupAttrIndices:
                groupTuple += (row[j],)

            if len(currGVs) == 0:
                break
            attrValue = row[j]
            
            for gV, conds in gVs.items():
                for cond in conds:
                    if attrIndex[cond['attribute']] == j:
                        if not eval("attrValue"+str(cond['operator'])+str(cond['value'])):
                            currGVs.remove(gV)

        if groupTuple not in groups:
            groups[groupTuple] = {}

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
        
except (Exception, pg2.DatabaseError) as error:
    print("Error executing query: ", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("Query executed and connection closed successfully!\n")