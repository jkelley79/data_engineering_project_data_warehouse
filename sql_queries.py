import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
    )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year INTEGER
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    sp_songplay_id BIGINT IDENTITY(0,1),
    sp_start_time TIMESTAMP,
    sp_user_id INTEGER,
    sp_level VARCHAR,
    sp_song_id VARCHAR,
    sp_artist_id VARCHAR,
    sp_session_id INTEGER,
    sp_location VARCHAR,
    sp_user_agent VARCHAR
    )
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    u_user_id int PRIMARY KEY,
    u_first_name VARCHAR,
    u_last_name VARCHAR,
    u_gender VARCHAR,
    u_level VARCHAR
    )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    s_song_id VARCHAR PRIMARY KEY,
    s_title VARCHAR,
    s_artist_id VARCHAR,
    s_year INTEGER,
    s_duration FLOAT,
    UNIQUE (s_song_id, s_artist_id, s_duration)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    a_artist_id VARCHAR PRIMARY KEY,
    a_name VARCHAR,
    a_location VARCHAR,
    a_latitude FLOAT,
    a_longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    t_start_time TIMESTAMP, 
    t_hour INTEGER, 
    t_day INTEGER, 
    t_week INTEGER, 
    t_month INTEGER, 
    t_year INTEGER, 
    t_weekday INTEGER
)
""")


# STAGING TABLES

staging_events_copy = ("""
copy staging_events
from '{}'
iam_role '{}' 
json '{}';
""").format(config['S3']['LOG_DATA'],config['IAM_ROLE']['ARN'],config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs
from '{}'
iam_role '{}' 
json 'auto';
""").format(config['S3']['SONG_DATA'],config['IAM_ROLE']['ARN'])

# FINAL TABLES

# Insert into the songplays table by joing events with songs and selecting distinct rows to eliminate duplicates.
songplay_table_insert = ("""
insert into songplays (sp_artist_id, sp_level, sp_location, sp_session_id, sp_song_id, sp_start_time, sp_user_agent, sp_user_id)
select distinct s.artist_id, e.level, e.location, e.sessionid, s.song_id, timestamp 'epoch' + e.ts/1000 * interval '1 second', e.useragent, e.userid from staging_events e
join staging_songs s
on s.artist_name = e.artist and e.song = title and e.length = s.duration
where page = 'NextSong'
""")

# Insert into the users table eliminating duplicates by using the row_number function and only picking the first partition of user_id.
# Using the distinct keyword would not work here since level is not distinct within the user_id so you still end up with multiple rows
# Distinct will net you 104 rows from the sample data set but this method will drop that further to 96
user_table_insert = ("""
insert into users (u_user_id, u_first_name, u_last_name, u_gender, u_level)
select userId, firstname, lastname, gender, level
From 
( 
 select userId, firstname, lastname, gender, level, row_number() over (partition by userId order by level desc) as rno from staging_events 
 where userId is not null and page = 'NextSong'
 order by userId
) 
where rno = 1
""")

# Insert into the songs table eliminating duplicates by using the row_number function and only picking the first partition of user_id.
# Using the distinct keyword would not work here since duration or year may be different across distinct song rows
# The sample data didn't have any duplicates but this will eliminate any future ones
song_table_insert = ("""
insert into songs (s_artist_id, s_duration, s_song_id, s_title, s_year)
select artist_id, duration, song_id, title, year
from 
( 
 select song_id, title, artist_id, year, duration, row_number() over (partition by song_id,artist_id,duration order by duration desc) as rno 
 from staging_songs 
 where song_id is not null and artist_id is not null
 order by artist_id 
) 
where rno = 1
""")

# Insert into the artists table eliminating duplicates by using the row number function and only picking the first partition of artist_id
# Distinct will not eliminate all duplicates for this insert either as it will return 10025 rows from the sample data set where using
# row_number will drop that further to 9553 rows
artist_table_insert = ("""
insert into artists (a_artist_id, a_latitude, a_longitude, a_location, a_name)
select artist_id, artist_latitude, artist_longitude, artist_location, artist_name
from 
( 
 select artist_id, artist_latitude, artist_longitude, artist_location, artist_name, row_number() over (partition by artist_id order by artist_name desc) as rno 
 from staging_songs 
 where artist_id is not null
 order by artist_id 
) 
where rno = 1
""")

# Select distinct timestamp values from events in order to populate the time table
time_table_insert = ("""
insert into time (t_start_time, t_hour, t_day, t_month, t_week, t_weekday, t_year)
select start_time, hour, day, month, week, weekday, year from (
select distinct(ts) as tstamp,
timestamp 'epoch' + tstamp/1000 * interval '1 second' as start_time,
extract(hour from start_time) as hour, 
extract(day from start_time) as day,
extract(month from start_time) as month,
extract(week from start_time) as week,
extract(weekday from start_time) as weekday,
extract(year from start_time) as year
from staging_events
where page = 'NextSong' )
""")

# VALIDATION QUERIES

songplay_validate = "select count(*) from songplays"
user_validate = "select count(*) from users"
song_validate = "select count(*) from songs"
artist_validate = "select count(*) from artists"
time_validate = "select count(*) from time"

events_validate = "select count(*) from staging_events"
nextsong_events_validate = "select count(*) from staging_events where page = 'NextSong'"
songs_validate = "select count(*) from staging_songs"

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
validation_queries  = [songplay_validate, user_validate, song_validate, artist_validate, time_validate]
staging_validation_queries  = [events_validate, nextsong_events_validate, songs_validate]