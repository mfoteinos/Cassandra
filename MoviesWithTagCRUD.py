import pandas as pd
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

from db_connection import Connection
from YearsAndTags import CreateDataFrame

movieAndTagsDf = CreateDataFrame('CSV\\tagsAndMovies.csv')

movieAndTagsDf['tag'] = movieAndTagsDf['tag'].fillna("none")

def Create():
    print('========================================')
    # Try...
    try:
        connection = Connection()
        insert = connection.session.prepare('INSERT INTO movies_with_tag (tag, rating, movieid, title, year) VALUES (?, ?, ?, ?, ?)')
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for i, row in movieAndTagsDf.iterrows():
            if i % 3000 == 0:
                output = connection.session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            if i == len(movieAndTagsDf) - 1:
                output = connection.session.execute(batch)
            print(row)
            batch.add(insert, (row[6], row[4], row[0], row[1], row[2]))
        
        
    except Exception as e: 
        print(e)
        print('Failure')
    else:
        print('Data created ')
        print('Success')
        print('Closing connection (up to 10s)')
    finally:
        connection.close()
    print('========================================')
    
Create()

connection = Connection()
# output = connection.session.execute('TRUNCATE table movies_with_tag;')
output = connection.session.execute('SELECT COUNT(*) FROM movies_with_tag;')

print(output)