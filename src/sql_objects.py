import configparser
import psycopg2
from create_resources import config_file

# CONFIG
config = configparser.ConfigParser()
config.read(config_file)

def create_connection():
    # Read CFG File
    config = configparser.ConfigParser()
    config.read(config_file)

    # Variables to create connection to Redshift Cluster
    host = config.get('CLUSTER', 'HOST')
    db_name = config.get('CLUSTER', 'DB_NAME')
    db_username = config.get('CLUSTER', 'DB_USER')
    db_password = config.get('CLUSTER', 'DB_PASSWORD')
    port = config.getint('CLUSTER', 'DB_PORT')

    # Connecting to Redshift Cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, db_name, db_username, db_password, port))
    cur = conn.cursor()
    return cur, conn

def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

def execute_query(cur, conn, query):
    try:
        cur.execute(query)
        rows = cur.fetchall()
        for row in rows:
            print(row)
        conn.commit()
    except Exception as e:
        print(e)

def get_query(question_number):
    """Function to return a CQL query based on a question number

    Args:
        question_number ([int]): [Question number the user wants to answer]

    Returns:
        [string]: [SQL Query]
    """
    query = {
              1: "select a.name as artist_name, Count(1) As Plays from songplays as sp inner join artists as a on sp.artist_id = a.artist_id Group By Name Order By Plays Desc Limit 10"
             ,2: "select s.title as song_title, Count(1) As Plays from songplays as sp inner join songs as s on sp.song_id = s.song_id Group By title Order By Plays Desc Limit 10"
             ,3: "select t.hour, Count(1) As Plays from songplays as sp inner join time as t on sp.start_time = t.start_time Group By t.hour Order By hour Desc"
            }
    if question_number == 4:
        query = str(input("Please enter your query: "))
        return query
    else:
        return query.get(question_number,"Invalid question number!")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender CHAR,
        session_item INT,
        last_name VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        session_id INT,
        song VARCHAR ,
        status INT,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        artist_id VARCHAR,
        artist_location VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_name VARCHAR,
        duration FLOAT,
        num_songs INT,
        song_id VARCHAR,
        title VARCHAR,
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id INT IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id VARCHAR NOT NULL,
        level VARCHAR NOT NULL,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id VARCHAR NOT NULL PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender CHAR,
        level VARCHAR NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR NOT NULL PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT,
        duration INT
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR NOT NULL PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE time (
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
    FROM '{}'
    IAM_ROLE '{}'
    JSON '{}';
""").format(config.get('S3', 'LOG_DATA'), config.get('SECURITY', 'ROLE_ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs
    FROM '{}'
    IAM_ROLE '{}'
    FORMAT AS JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('SECURITY', 'ROLE_ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 Second ',
           user_id,
           level,
           song_id,
           artist_id,
           session_id,
           location,
           user_agent
    FROM staging_events e
    Inner Join staging_songs s ON e.song = s.title AND e.artist = s.artist_name
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT (user_id),
           first_name,
           last_name,
           gender,
           level
    FROM staging_events
    WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id),
           title,
           artist_id,
           year,
           duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT (artist_id),
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT (TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 Second ') as ts_timestamp,
           EXTRACT(HOUR FROM ts_timestamp),
           EXTRACT(DAY FROM ts_timestamp),
           EXTRACT(WEEK FROM ts_timestamp),
           EXTRACT(MONTH FROM ts_timestamp),
           EXTRACT(YEAR FROM ts_timestamp),
           EXTRACT(DOW FROM ts_timestamp)
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]