import pandas as pd
import time
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

from db_connection import Connection
from YearsAndTags import CreateDataFrame

movieAndTagsDf = CreateDataFrame('CSV\\tagsAndMovies.csv')

# Adds the word "none" to all the rows with no tag
movieAndTagsDf['tag'] = movieAndTagsDf['tag'].fillna("none")

# Creates and adds the data from the data frame to the "movies_with_tag" table
def Create():
    print('========================================')
    # Try...
    try:
        # Creates a connection
        connection = Connection()
        # Prepares the query
        insert = connection.session.prepare('INSERT INTO movies_with_tag (tag, rating, movieid, title, year) VALUES (?, ?, ?, ?, ?)')
        # Create a new batch statement
        batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
        
        # Sets the current time as the time started
        started = time.time()
        
        # For each row in the data frame...
        for i, row in movieAndTagsDf.iterrows():
            # For each 3000 insert...
            if i % 3000 == 0:
                # Execute the query
                output = connection.session.execute(batch)
                # Create a new batch statement
                batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
            # If it is the last group of insert...
            if i == len(movieAndTagsDf) - 1:
                # Execute the query
                output = connection.session.execute(batch)
            # Add the row data to the batch statement
            batch.add(insert, (row[6], row[4], row[0], row[1], row[2]))
        
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
        print('Data created ')
        print('Success')
        print('Closing connection (up to 10s)')
    # Always...
    finally:
        # Closes the connection
        connection.close()
    print('========================================')
    
# Gets the first top movies with the given tag according to the given limit
# <tag> The tag
# <limit> The limit
def Get(tag, limit):
    print('========================================\n')

    # Try...
    try:
        # Creates a connection
        connection = Connection()
        # Prepares the query
        prepared = connection.session.prepare(
            f'SELECT * FROM movies_with_tag WHERE tag = \'{tag}\' ORDER BY rating DESC LIMIT {limit};'
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
            # print(f'{offset} Movie:\n Title: {row.title}\n Year: {row.year}\n Tag: {row.tag.capitalize()}\n Rating: {rating}\n')
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
output = connection.session.execute('TRUNCATE table movies_with_tag;')
output = connection.session.execute('SELECT COUNT(*) FROM movies_with_tag;')

print(output)

Get("based on a book", 550)
