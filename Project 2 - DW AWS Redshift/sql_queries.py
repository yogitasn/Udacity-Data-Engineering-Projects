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
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                        artist VARCHAR,
                                        auth VARCHAR,
                                        firstName VARCHAR,
                                        gender VARCHAR,
                                        itemInSession INTEGER,
                                        lastName VARCHAR,
                                        length FLOAT,
                                        level  VARCHAR,
                                        location VARCHAR,
                                        method VARCHAR,
                                        page VARCHAR,
                                        registration BIGINT,
                                        sessionId INTEGER,
                                        song VARCHAR,
                                        status VARCHAR,
                                        ts BIGINT,
                                        userAgent VARCHAR,
                                        userId INTEGER);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                         num_songs INTEGER,
                                         artist_id  VARCHAR,
                                         artist_latitude FLOAT,
                                         artist_longitude FLOAT,
                                         artist_location VARCHAR,
                                         artist_name  VARCHAR,
                                         song_id VARCHAR,
                                         title VARCHAR,
                                         duration FLOAT,
                                         year INTEGER);

""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
                                     songplay_id INTEGER identity(0,1) PRIMARY KEY,
                                     start_time TIMESTAMP, 
                                     user_id INTEGER, 
                                     level VARCHAR, 
                                     song_id VARCHAR, 
                                     artist_id VARCHAR,
                                     session_id INTEGER,
                                     location VARCHAR, 
                                     user_agent VARCHAR);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                                     user_id     INTEGER PRIMARY KEY,
                                     first_name  VARCHAR,
                                     last_name   VARCHAR,
                                     gender      VARCHAR,
                                     level       VARCHAR NOT NULL);
                        
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
                                song_id VARCHAR PRIMARY KEY,
                                title   VARCHAR NOT NULL,
                                artist_id VARCHAR NOT NULL, 
                                year INTEGER, 
                                duration FLOAT);
""")

artist_table_create = ("""CREATE TABLE artists (
                          artist_id    VARCHAR PRIMARY KEY,
                          name         VARCHAR NOT NULL,
                          location     VARCHAR,
                          latitude    FLOAT,
                          longitude    FLOAT
                        );

""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                                start_time TIMESTAMP, 
                                hour INTEGER,
                                day INTEGER, 
                                week INTEGER, 
                                month INTEGER,
                                year INTEGER, 
                                weekday INTEGER
                           );
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {} 
                                iam_role {} json {} """).format(config['S3']['LOG_DATA'], 
                                config['IAM_ROLE']['ARN'], 
                                config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy staging_songs from {}
        iam_role {} 
        compupdate off region 'us-west-2'
        JSON 'auto' truncatecolumns;""").format(config['S3']['SONG_DATA'], 
                                config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time,user_id, level, song_id, artist_id,                                             session_id, location, user_agent) \
                                    SELECT DISTINCT
                                    TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1second' as start_time,
                                    se.userId,
                                    se.level,
                                    ss.song_id,
                                    ss.artist_id,
                                    se.sessionId,
                                    se.location,
                                    se.userAgent
                                    FROM staging_events se, staging_songs ss
                                    WHERE se.page = 'NextSong'
                                    AND se.song = ss.title
                                    AND se.artist = ss.artist_name
                                    AND se.length = ss.duration

""")

user_table_insert = ("""INSERT INTO users(user_id,first_name,last_name, gender, level) \
                                SELECT DISTINCT userId,
                                       firstName,
                                       lastName,
                                       gender,
                                       level
                                  FROM staging_events
                                  WHERE userId IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) \
                                    SELECT DISTINCT song_id,
                                           title,
                                           artist_id,
                                           year,
                                           duration
                                    FROM staging_songs
                                    WHERE song_id is NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude) \
                                     SELECT DISTINCT artist_id,
                                            artist_name,
                                            artist_location,
                                            artist_latitude,
                                            artist_longitude
                                     FROM staging_songs
                                     WHERE artist_id is NOT NULL;
""")

time_table_insert = ("""INSERT INTO  time(start_time, hour, day, week, month, year, weekday) \
                                    Select DISTINCT start_time
                                    ,EXTRACT(HOUR FROM start_time) As hour
                                    ,EXTRACT(DAY FROM start_time) As day
                                    ,EXTRACT(WEEK FROM start_time) As week
                                    ,EXTRACT(MONTH FROM start_time) As month
                                    ,EXTRACT(YEAR FROM start_time) As year
                                    ,EXTRACT(DOW FROM start_time) As weekday
                                    FROM (SELECT '1970-01-01'::date + ts/1000 * interval '1 second' as start_time
                                    FROM staging_events) ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
