import pandas as pd
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

from db_connection import Connection
from YearsAndTags import CreateDataFrame

moviesDf = CreateDataFrame('CSV\moviesWithYears.csv')


def Create():
    print('========================================')
    # Try...
    try:
        connection = Connection()
        insert = connection.session.prepare('INSERT INTO genre_by_year (genre, year, movieid, rating, title) VALUES (?, ?, ?, ?, ?)')
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for i, row in moviesDf.iterrows():
            if i % 3000 == 0:
                output = connection.session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            if i == len(moviesDf) - 1:
                output = connection.session.execute(batch)
                
            batch.add(insert, (row[3],row[2],row[0],row[4],row[1]))
        
    except Exception as e: 
        print(e)
        print('Failure')
    else:
        print('Data created')
        print('Success')
        print('Closing connection (up to 10s)')
    finally:
        connection.close()
    print('========================================')
   
   
def Get(genre, orderBy):
    print('========================================\n')

    # Try...
    try:
        connection = Connection()
        output = connection.session.execute(
            f'SELECT * from genre_by_year WHERE genre = \'{genre}\' ORDER BY {orderBy} DESC;'
        )
        offset = 0
        for row in output:
            rating = round(row.rating, 2)
            print(f'{offset} Movie:\n Title: {row.title}\n Year: {row.year}\n Genre: {row.genre} Rating: {rating}\n')
            offset = offset + 1
    except Exception as e: 
        print(e)
        print('Failure')
    else:

        print('Success')
        print('Closing connection (up to 10s)')
    finally:
        connection.close()
    print('========================================')
   
    
# Create()
    
# connection = Connection()
# output = connection.session.execute('TRUNCATE table genre_by_year;')
# output = connection.session.execute('SELECT COUNT(*) FROM genre_by_year;')

# print(output)


Get("Adventure", "year")

print('yeah')
