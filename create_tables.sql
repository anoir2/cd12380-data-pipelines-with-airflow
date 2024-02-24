DROP table IF EXISTS public.artists;

DROP table IF EXISTS public.songplays;

DROP table IF EXISTS public.songs;

DROP table IF EXISTS public.staging_events;

DROP table IF EXISTS public.staging_songs;

DROP table IF EXISTS public."time";

DROP table IF EXISTS public.users;

CREATE TABLE public.artists (
        artist_id VARCHAR(512) NOT NULL PRIMARY KEY,
        name VARCHAR(512) NOT NULL,
        location VARCHAR(512), 
        latitude DOUBLE PRECISION, 
        longitude DOUBLE PRECISION
);

CREATE TABLE public.songplays (
	songplay_id VARCHAR(32),
	start_time TIMESTAMP, 
	user_id INTEGER DISTKEY, 
	level VARCHAR(4), 
	song_id VARCHAR(512), 
	artist_id VARCHAR(512),
	session_id INTEGER, 
	location VARCHAR(512),
	user_agent VARCHAR(256)
);

CREATE TABLE public.songs (
	song_id VARCHAR(512) NOT NULL PRIMARY KEY,
	title VARCHAR(512) NOT NULL,
	artist_id VARCHAR(512) NOT NULL, 
	year INTEGER NOT NULL, 
	duration DOUBLE PRECISION NOT NULL
);

CREATE TABLE public.staging_events (
	artist VARCHAR(512) DISTKEY,
	auth VARCHAR(16) ,
	firstname VARCHAR(512) , 
	gender VARCHAR(1), 
	item_in_session INTEGER ,
	lastname VARCHAR(512) , 
	length FLOAT, 
	level VARCHAR(32) , 
	location VARCHAR(512), 
	method VARCHAR(8) , 
	page VARCHAR(16) , 
	registration VARCHAR(512), 
	sessionid INTEGER,
	song VARCHAR(512), 
	status INTEGER, 
	ts BIGINT,
	useragent VARCHAR(256), 
	userid INTEGER
);

CREATE TABLE public.staging_songs (
	num_songs INTEGER, 
	artist_id VARCHAR(512) DISTKEY, 
	artist_latitude DOUBLE PRECISION, 
	artist_longitude DOUBLE PRECISION,
	artist_location VARCHAR(512),
	artist_name VARCHAR(512),
	song_id VARCHAR(512), 
	title VARCHAR(512), 
	duration DOUBLE PRECISION, 
	year integer  
);

CREATE TABLE public."time" (
	start_time TIMESTAMP NOT NULL PRIMARY KEY,
	hour INTEGER NOT NULL,
	day INTEGER NOT NULL,
	week INTEGER NOT NULL,
	month INTEGER NOT NULL,
	year INTEGER NOT NULL,
	weekday INTEGER NOT NULL
) ;

CREATE TABLE public.users (
	user_id INTEGER NOT NULL PRIMARY KEY,
	first_name VARCHAR(50) NOT NULL,
	last_name VARCHAR(50) NOT NULL, 
	gender VARCHAR(1), 
	level VARCHAR(4)
);
