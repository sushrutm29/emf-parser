import psycopg2 as pg2

def readDb():
    try:
        connection = pg2.connect(user = "postgres",
                                password = "grespost123",
                                host = "127.0.0.1",
                                port = "5432",
                                database = "postgres")
        cursor = connection.cursor()
        select_query = '''SELECT * from sales'''
        cursor.execute(select_query)
        mobile_records = cursor.fetchall()

    except (Exception, pg2.DatabaseError) as error:
        print("Error executing query: ", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Connection closed!\n")
        return mobile_records    