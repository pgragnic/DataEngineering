class SqlQueries:
    
    # INSERT INTO TABLES
    
    songplay_table_insert = ("""
        SELECT
        md5(events.sessionid || events.start_time) songplay_id,
        events.start_time,
        events.userid,
        events.level,
        songs.song_id,
        songs.artist_id,
        events.sessionid,
        events.location,
        events.useragent
        FROM
        (
            SELECT
            TIMESTAMP 'epoch' + ts / 1000 * interval '1 second' AS start_time,
            *
            FROM
            staging_events
            WHERE
            page = 'NextSong'
        ) events
        LEFT JOIN staging_songs songs ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration;
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

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
        CREATE TABLE IF NOT EXISTS staging_events
            (
                artist varchar,
                auth varchar,
                firstName varchar,
                gender varchar,
                itemInSession integer,
                lastName varchar,
                length numeric,
                level varchar,
                location varchar,
                method varchar,
                page varchar,
                registration numeric,
                sessionId integer,
                song varchar,
                status integer,
                ts bigint,
                userAgent varchar,
                userId integer
            );
    """)

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs
            (
                num_songs integer,
                artist_id varchar,
                artist_latitude numeric,
                artist_longitude numeric,
                artist_location varchar,
                artist_name varchar,
                song_id varchar,
                title varchar,
                duration numeric,
                year integer
            );
    """)

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays 
            (
                songplay_id varchar(32) NOT NULL,
                start_time timestamp NOT NULL,
                user_id int4 NOT NULL,
                "level" varchar(256),
                song_id varchar(256),
                artist_id varchar(256),
                session_id int4,
                location varchar(256),
                user_agent varchar NOT NULL,
                UNIQUE (start_time, user_id)
            );
    """)

    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users
            (
                user_id int PRIMARY KEY,
                first_name varchar NOT NULL,
                last_name varchar NOT NULL,
                gender varchar NOT NULL,
                level varchar NOT NULL
            );
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs
            (
                song_id varchar PRIMARY KEY,
                title varchar NOT NULL,
                artist_id varchar NOT NULL,
                year int NOT NULL,
                duration numeric NOT NULL
            );
    """)

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists
            (
                artist_id varchar PRIMARY KEY,
                name varchar NOT NULL,
                location varchar,
                latitude numeric,
                longitude numeric
            );
    """)

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time \
            (
                start_time timestamp PRIMARY KEY,
                hour int NOT NULL,
                day int NOT NULL,
                week int NOT NULL,
                month varchar NOT NULL,
                year int NOT NULL,
                weekday int NOT NULL
            );
    """)

    create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
    drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]