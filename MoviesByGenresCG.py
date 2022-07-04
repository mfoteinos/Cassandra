import pandas as pd
import time
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

from db_connection import Connection
from YearsAndTags import CreateDataFrame

moviesDf = CreateDataFrame('CSV\moviesWithYears.csv')

# Create and add to the "genre_by_year" table the data from the data frame
def Create():
    print('========================================')
    # Try...
    try:
        # Creates a connection
        connection = Connection()
        # Prepares the query
        insert = connection.session.prepare('INSERT INTO genre_by_year (genre, year, movieid, rating, title) VALUES (?, ?, ?, ?, ?)')
        # Create a new batch statement
        batch = BatchStatement(consistency_level=ConsistencyLevel.ALL)
        
        # Sets the current time as the time started
        started = time.time()
        
        # For each row in the data frame...
        for i, row in moviesDf.iterrows():
            # For each 3000 insert...
            if i % 3000 == 0:
                # Execute the query
                output = connection.session.execute(batch)
                # Create a new batch statement
                batch = BatchStatement(consistency_level=ConsistencyLevel.ALL)
            # If it is the last group of insert...
            if i == len(moviesDf) - 1:
                # Execute the query
                output = connection.session.execute(batch)
                
            # Add the row data to the batch statement
            batch.add(insert, (row[3],row[2],row[0],row[4],row[1]))
       
        # Sets the current time as the time finished
        finished = time.time()
        
        print(round(finished - started, 5))
         
    # If there is an error...
    except Exception as e: 
        # Print the exception
        print(e)
        print('Failure')
    # Else...
    else:
        print('Data created')
        print('Success')
        print('Closing connection (up to 10s)')
    # Always...
    finally:
        # Closes the connection
        connection.close()
    print('========================================')
   
   
# Create and add to the "genre_by_rating" table the data from the data frame
def CreateByRating():
    print('========================================')
    # Try...
    try:
        # Creates a connection
        connection = Connection()
        # Prepares the query
        insert = connection.session.prepare('INSERT INTO genre_by_rating (genre, year, movieid, rating, title)  VALUES (?, ?, ?, ?, ?)')
        # Create a new batch statement
        batch = BatchStatement(consistency_level=ConsistencyLevel.ALL)
        
        # Sets the current time as the time started
        started = time.time()
        
        # For each row in the data frame...
        for i, row in moviesDf.iterrows():
            # For each 3000 insert...
            if i % 3000 == 0:
                # Execute the query
                output = connection.session.execute(batch)
                # Create a new batch statement
                batch = BatchStatement(consistency_level=ConsistencyLevel.ALL)
            # If it is the last group of insert...
            if i == len(moviesDf) - 1:
                # Execute the query
                output = connection.session.execute(batch)
            # Add the row data to the batch statement
            batch.add(insert, (row[3],row[2],row[0],row[4],row[1]))
        
        # Sets the current time as the time finished
        finished = time.time()
        
        print(round(finished - started, 5))
        
    # If there is an error...
    except Exception as e: 
        print(e)
        print('Failure')
    else:
        print('Data created')
        print('Success')
        print('Closing connection (up to 10s)')
    # Always...
    finally:
        # Closes the connection
        connection.close()
    print('========================================')
   
   
# Gets the movies with the given genre and ordered accordingly to the table's name
# <genre> The genre 
# <orderBy> The order by column name
def Get(genre, orderBy):
    print('========================================\n')

    # Try...
    try:
        # Creates a connection
        connection = Connection()
        # Prepares the query
        prepared = connection.session.prepare(
            f'SELECT * from genre_by_{orderBy} WHERE genre = \'{genre}\' ORDER BY {orderBy} DESC;'
        )
        # Sets the consistency level
        prepared.consistency_level = ConsistencyLevel.ALL
        # Sets the current time as the time started
        started = time.time()
        # Executes the query
        output = connection.session.execute(prepared)
        # Sets the current time as the time finished
        finished = time.time()
        # Sets the index to 0
        offset = 0
        # For each row in the output...
        for row in output:
            # Rounds the rating to 2 decimal digits
            rating = round(row.rating, 2)
            # Prints the movie data
            # print(f'{offset} Movie:\n Title: {row.title}\n Year: {row.year}\n Genre: {row.genre}\n Rating: {rating}\n')
            # Increments the index by one
            offset = offset + 1
            
        print(round(finished - started, 5))
            
    # If there is an error...
    except Exception as e: 
        # Print the exception
        print(e)
        print('Failure')
    # Else...
    else:
        print('Success')
        print('Closing connection (up to 10s)')
    # Always...
    finally:
        # Closes the connection
        connection.close()
    print('========================================')
   
    
Create()
    
connection = Connection()
output = connection.session.execute('TRUNCATE table genre_by_year;')
output = connection.session.execute('SELECT COUNT(*) FROM genre_by_year;')

print(output)

CreateByRating()

connection = Connection()
output = connection.session.execute('TRUNCATE table genre_by_rating;')
output = connection.session.execute('SELECT COUNT(*) FROM genre_by_rating;')

print(output)

Get("Mystery", "year")
