CREATE TABLE IF NOT EXISTS public.staging_events (
  artist varchar,
  auth varchar,
  firstname varchar,
  gender varchar,
  iteminsession integer,
  lastname varchar,
  length float,
  level varchar,
  location varchar,
  method varchar,
  page varchar,
  registration bigint,
  sessionid integer,
  song varchar,
  status integer,
  ts bigint,
  useragent varchar,
  userid integer
);


CREATE TABLE IF NOT EXISTS public.staging_songs (
  num_songs integer,
  artist_id varchar,
  artist_latitude NUMERIC (9, 5),
  artist_longitude NUMERIC (9, 5),
  artist_location varchar,
  artist_name varchar,
  song_id varchar,
  title varchar,
  duration float,
  year integer
);


CREATE TABLE IF NOT EXISTS public.songplay
  (songplay_id varchar PRIMARY KEY, 
  start_time TIMESTAMP distkey, 
  userid integer, 
  level varchar  ,
  song_id varchar sortkey, 
  artist_id varchar, 
  session_id varchar , 
  location varchar, 
  useragent text);


CREATE TABLE IF NOT EXISTS public.users 
  (userid integer PRIMARY KEY sortkey, 
  firstname varchar, 
  lastname varchar, 
  gender varchar, 
  level varchar);


CREATE TABLE IF NOT EXISTS public.songs  
  (song_id varchar PRIMARY KEY sortkey, 
  title varchar , 
  artist_id varchar ,  
  year integer , 
  duration float);



CREATE TABLE IF NOT EXISTS public.artists 
  (artist_id varchar PRIMARY KEY sortkey, 
  artist_name varchar, 
  artist_location varchar, 
  artist_latitude  NUMERIC (9, 5), 
  artist_longitude  NUMERIC (9, 5));


CREATE TABLE IF NOT EXISTS public.time 
  (start_time timestamp PRIMARY KEY NOT NULL sortkey, 
  hour integer NOT NULL , 
  day integer NOT NULL ,  
  week integer NOT NULL , 
  month integer NOT NULL ,  
  year  integer NOT NULL ,  
  weekday integer NOT NULL) diststyle all;
