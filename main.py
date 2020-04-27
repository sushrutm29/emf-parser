from mfstruct import create_struct
import psycopg2 as pg2

groupAttrIndices, fVs_temp, gVs, selects, attrIndex = create_struct()

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

    groups = {}

    # Iterating through each row
    for i in range(cursor.rowcount):
        row = cursor.fetchone()
        groupTuple = tuple()
        
        # Forming a tuple of grouping attributes
        for j in range(7):
            if j in groupAttrIndices:
                groupTuple += (row[j],)

        if groupTuple not in groups:
            groups[groupTuple] = []
        
        currGVs = []
        for gV in gVs.keys():
            currGVs.append(gV)

        # This loop identifies which, if any, grouping variable condition is satisfied by current row
        for j in range(7):
            if len(currGVs) == 0:
                break
            attrValue = row[j]
            
            for gV, conds in gVs.items():
                for cond in conds:
                    if attrIndex[cond['attribute']] == j:
                        if not eval("attrValue"+str(cond['operator'])+str(cond['value'])):
                            currGVs.remove(gV)

        # Skip current row, if it does not satisfy any grouping variable condition
        if len(currGVs) == 0:
            continue
    
except (Exception, pg2.DatabaseError) as error:
    print("Error executing query: ", error)

finally:
    if connection:
        cursor.close()
        connection.close()
        print("Query executed and connection closed successfully!\n")