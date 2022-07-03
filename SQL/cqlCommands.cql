CREATE TABLE IF NOT EXISTS genre_by_year( 
  genre      TEXT,
  movieId     INT, 
  year        INT, 
  title      TEXT,
  rating     FLOAT,
  PRIMARY KEY ((genre), year, movieId)
) WITH CLUSTERING ORDER BY (year DESC, movieId ASC); 

DROP TABLE IF EXISTS "musemovies".genre_by_year;

TRUNCATE table genre_by_year;

INSERT INTO genre_by_year (genre, year, movieid, rating, title) 
        VALUES (%s, %s, %s, %s, %s)

SELECT * FROM genre_by_year;

SELECT * from genre_by_year WHERE genre = 'Adventure' ORDER BY year DESC;

CREATE TABLE IF NOT EXISTS movies_with_tag ( 
  tag        TEXT,
  movieId     INT, 
  title      TEXT,
  year        INT, 
  rating     FLOAT,
  PRIMARY KEY ((tag), rating, movieId)
) WITH CLUSTERING ORDER BY (rating  DESC, movieId ASC); 

DROP TABLE IF EXISTS "musemovies".movies_with_tag;

TRUNCATE table movies_with_tag;

INSERT INTO movies_with_tag (tag, rating, movieid, title, year) 
        VALUES (%s, %s, %s, %s, %s)

SELECT * FROM movies_with_tag;

SELECT * FROM movies_with_tag WHERE tag = 'comedy' ORDER BY rating DESC LIMIT 5;
