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


CREATE TABLE IF NOT EXISTS genre_by_rating( 
  genre      TEXT,
  movieId     INT, 
  year        INT, 
  title      TEXT,
  rating     FLOAT,
  PRIMARY KEY ((genre), rating, movieId)
) WITH CLUSTERING ORDER BY (rating DESC, movieId ASC); 

DROP TABLE IF EXISTS "musemovies".genre_by_rating;

TRUNCATE table genre_by_rating;

INSERT INTO genre_by_rating (genre, year, movieid, rating, title) 
        VALUES (%s, %s, %s, %s, %s)

SELECT * FROM genre_by_rating;

SELECT * from genre_by_rating WHERE genre = 'Adventure' ORDER BY rating DESC;


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



CREATE TABLE musemovies.ratings_by_timestamp (
	movieid int,
	timestamp timestamp,
	userid int,
	score float,
	title text,
	PRIMARY KEY (movieid, timestamp, userid)
) WITH CLUSTERING ORDER BY (timestamp DESC, userid ASC)

CREATE TABLE musemovies.movie_info (
	movieid int,
	title text,
	genre text,
	score float,
	tags text,
	userid int,
	PRIMARY KEY (movieid, title, userid)
) WITH CLUSTERING ORDER BY (title ASC, userid ASC)
