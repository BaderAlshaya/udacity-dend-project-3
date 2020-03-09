import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_fact"
user_table_drop = "DROP TABLE IF EXISTS user_dim"
song_table_drop = "DROP TABLE IF EXISTS song_dim"
artist_table_drop = "DROP TABLE IF EXISTS artist_dim"
time_table_drop = "DROP TABLE IF EXISTS time_dim"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
        id INT IDENTITY(0,1) NOT NULL,
        artist VARCHAR,
        auth VARCHAR,
        firstname VARCHAR,
        gender VARCHAR,
        iteminsession INT,
        lastname VARCHAR,
        length DECIMAL,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionid INT,
        song VARCHAR,
        status INT,
        ts BIGINT NOT NULL,
        useragent VARCHAR,
        userid INT
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT NOT NULL,
        artist_id VARCHAR NOT NULL,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location VARCHAR,
        artist_name VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL,
        title VARCHAR NOT NULL,
        duration DECIMAL NOT NULL,
        year INT NOT NULL
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_fact (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id IN T NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INT NOT NULL,
        location VARCHAR NOT NULL,
        user_agent VARCHAR NOT NULL
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_dim (
        user_id INT NOT NULL PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR NOT NULL,
        level VARCHAR NOT NULL
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_dim (
        song_id VARCHAR NOT NULL PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT NOT NULL,
        duration DECIMAL NOT NULL
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_dim (
        artist_id VARCHAR NOT NULL PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR NOT NULL,
        lattitude DECIMAL NOT NULL,
        longitude DECIMAL NOT NULL
    );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_dim (
        start_time TIMESTAMP NOT NULL PRIMARY KEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL, 
        weekday INT NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events 
    FROM {}
    CREDENTIALS 'aws_iam_role=arn:aws:iam::012345678901:role/dwhRole'
    JSON {}
    REGION 'us-west-2';
""").format(config.get('S3','LOG_DATA'),
            config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role=arn:aws:iam::012345678901:role/dwhRole'
    JSON 'auto'
    REGION 'us-west-2';
""").format(config.get('S3','SONG_DATA'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay_fact (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT TO_TIMESTAMP(e.ts::TIMESTAMP/1000),
           e.userid,
           e.level,
           s.song_id,
           s.artist_id,
           e.sessionid,
           e.location,
           e.useragent
    FROM staging_events e
    JOIN staging_songs s
      ON e.song = s.title
     AND e.artist = s.artist_name;    
""")

user_table_insert = ("""
    INSERT INTO user_dim (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userid,
           firstname,
           lastname,
           gender,
           level
    FROM staging_events
    WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO song_dim (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
           title,
           artist_id,
           year,
           duration
    FROM staging_songs
    WHERE song_id IS NOT NUll;
""")

artist_table_insert = ("""
    INSERT INTO artist_dim (artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT artist_id,
           artist_name,
           location,
           lattitude,
           longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time_dim (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TO_TIMESTAMP(ts::TIMESTAMP/1000),
           EXTRACT(HOUR FROM TO_TIMESTAMP(ts::TIMESTAMP/1000)),
           EXTRACT(DAY FROM TO_TIMESTAMP(ts::TIMESTAMP/1000)),
           EXTRACT(WEEK FROM TO_TIMESTAMP(ts::TIMESTAMP/1000)),
           EXTRACT(MONTH FROM TO_TIMESTAMP(ts::TIMESTAMP/1000)),
           EXTRACT(YEAR FROM TO_TIMESTAMP(ts::TIMESTAMP/1000)),
           EXTRACT(ISODOW FROM TO_TIMESTAMP(ts::TIMESTAMP/1000))
    FROM staging_events
    WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
