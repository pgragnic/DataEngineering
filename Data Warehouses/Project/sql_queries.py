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
    CREATE TABLE IF NOT EXISTS staging_events
        (
            artist varchar,
            auth varchar NOT NULL,
            firstName varchar,
            gender varchar,
            itemInSession integer,
            lastName varchar,
            length numeric,
            level varchar NOT NULL,
            location varchar,
            method varchar NOT NULL,
            page varchar NOT NULL,
            registration numeric,
            sessionId integer,
            song varchar,
            status integer NOT NULL,
            ts bigint NOT NULL,
            userAgent varchar,
            userId integer
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
        (
            num_songs integer NOT NULL,
            artist_id varchar NOT NULL,
            artist_latitude numeric,
            artist_longitude numeric,
            artist_location varchar,
            artist_name varchar NOT NULL,
            song_id varchar NOT NULL,
            title varchar NOT NULL,
            duration numeric NOT NULL,
            year integer NOT NULL
        );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays 
        (
            songplay_id double precision DEFAULT nextval('songplays_seq') PRIMARY KEY,
            start_time bigint NOT NULL,
            user_id int NOT NULL,
            level varchar NOT NULL,
            song_id varchar,
            artist_id varchar,
            session_id int NOT NULL,
            location varchar,
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

# STAGING TABLES

staging_events_copy = (
        """ COPY {} FROM '{}'
        iam_role 'arn:aws:iam::909496952025:role/dwhRole'
        JSON 'auto ignorecase' ACCEPTINVCHARS
        """
        ).format("staging_events", "s3://udacity-dend/log_data")

staging_songs_copy = (
        """ COPY {} FROM '{}'
        iam_role 'arn:aws:iam::909496952025:role/dwhRole'
        JSON 'auto ignorecase' ACCEPTINVCHARS
        """
        ).format("staging_songs", "s3://udacity-dend/song_data")

# FINAL TABLES

songplay_table_insert = (""" """)

user_table_insert = ("""
        INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT 
            DISTINCT(userid) as user_id,
            firstname as first_name,
            lastname as last_name,
            gender,
            level
        FROM staging_events
        WHERE userid IS NOT NULL
""")

song_table_insert = ("""
        INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT
            DISTINCT (song_id) as song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
#insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_queries = [song_table_insert]
