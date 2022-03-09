
import sqlite3
import json


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# connect to the SQlite databases
def openConnection(pathToSqliteDb):
    connection = sqlite3.connect(pathToSqliteDb)
    connection.row_factory = dict_factory
    cursor = connection.cursor()
    return connection, cursor

def getAllRecordsInTable(table_name, pathToSqliteDb):
    conn, curs = openConnection(pathToSqliteDb)
    conn.row_factory = dict_factory
    curs.execute("SELECT * FROM {} ".format(table_name))
    # fetchall as result
    results = curs.fetchall()
    # close connection
    conn.close()
    return json.dumps(results)

def writeAllRecords(table_name, pathToSqliteDb, arraySize):
    conn, curs = openConnection(pathToSqliteDb)
    conn.row_factory = dict_factory
    curs.execute("SELECT * FROM {} ".format(table_name))

    print(f"Writing out records for table {table_name}")
    # if arraySize is 0 then we assume we are dumping the whole table
    # to a single file
    if arraySize == 0:
        # fetchall as result
        results = curs.fetchall()
        filename = 'results/'+table_name+'.json'
        with open(filename,'w') as the_file:
            the_file.write(json.dumps(results))   
    # else create json files with number of records defined by arraySize    
    else:
        results = curs.fetchmany(arraySize)
        i = 1
        while results:
            filename = 'results/'+table_name+'_'+str(i)+'.json'
            with open(filename,'w') as the_file:
                the_file.write(json.dumps(results))
                results = curs.fetchmany(arraySize)
                i += 1
    # close connection
    conn.close()

def sqliteToJson(pathToSqliteDb, arraySize=0):
    connection, cursor = openConnection(pathToSqliteDb)
    # select all the tables from the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    # for each of the tables , select all the records from the table
    for table_name in tables:
        # generate and save JSON files with the table name for each of the database tables
        # and dump records in table toresults folder
        writeAllRecords(table_name['name'], pathToSqliteDb, arraySize)
    # close connection
    connection.close()


if __name__ == '__main__':
    # modify path to sqlite db
    pathToSqliteDb = 'path/to/db.sqlite3'
    # if you want to break up your table into smaller files, use the ArraySize param
    # by default it will be 0 (meaning export table as one file)
    sqliteToJson(pathToSqliteDb,arraySize = 0)
