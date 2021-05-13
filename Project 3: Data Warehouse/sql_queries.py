import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop =  "DROP TABLE IF EXISTS songplay;"
user_table_drop =  "DROP TABLE IF EXISTS users;"
song_table_drop =  "DROP TABLE IF EXISTS songs;"
artist_table_drop =  "DROP TABLE IF EXISTS artists;"
time_table_drop =  "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
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
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
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
""")

songplay_table_create = ("""
CREATE TABLE songplay
 (songplay_id INTEGER IDENTITY (0, 1), 
        start_time TIMESTAMP distkey, 
        user_id integer, 
        level varchar  ,
        song_id varchar sortkey, 
        artist_id varchar, 
        session_id varchar , 
        location varchar, 
        user_agent text);
""")

user_table_create = ("""
CREATE TABLE users 
        (user_id integer sortkey, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level varchar);
""")

song_table_create = ("""
CREATE TABLE songs  
        (song_id varchar sortkey, 
        title varchar , 
        artist_id varchar ,  
        year integer , 
        duration float);
""")

artist_table_create = ("""
CREATE TABLE artists 
        (artist_id varchar sortkey, 
        name varchar, 
        location varchar, 
        latitude  NUMERIC (9, 5), 
        longitude  NUMERIC (9, 5));
""")

time_table_create = ("""
CREATE TABLE time 
        (start_time timestamp sortkey, 
        hour integer, 
        day integer,  
        week integer, 
        month integer,  
        year  integer,  
        weekday integer) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json {}
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {} 
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json 'auto'
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                      select timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
                      se.userid, se.level, so.song_id, so.artist_id, se.sessionid, se.location, se.useragent 
                        from staging_events se
                            left join staging_songs so on 
                            se.artist = so.artist_name
                            and se.song = so.title
                            and se.page = 'NextSong';""")

user_table_insert = ("""INSERT INTO users 
                     select distinct userid, firstname, lastname, gender, level
                        from staging_events;
                    """)

song_table_insert = ("""INSERT INTO songs  
                        select distinct song_id, title, artist_id, year, duration
                            from staging_songs
                    ;
                    """)

artist_table_insert = ("""INSERT INTO artists  
                        select distinct artist_id, artist_name,artist_location,artist_latitude, artist_longitude from staging_songs ;
                        """)

time_table_insert = ("""INSERT INTO time 
                        select distinct start_time
                        , EXTRACT(HOUR FROM start_time) as hour
                        , EXTRACT(DAY FROM start_time) as day
                        , EXTRACT(WEEK FROM start_time) as week
                        , EXTRACT(MONTH FROM start_time) as month
                        , EXTRACT(YEAR FROM start_time) as year
                        , EXTRACT(DOW FROM start_time) as weekday
                        from 
                        (SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time from staging_events se);
                        """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
