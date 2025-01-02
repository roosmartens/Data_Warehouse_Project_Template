import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config.get('IAM_ROLE', 'ARN')
log_json_path = config.get('S3', 'LOG_JSONPATH')

# table names
staging_events_table_name = "staging_events"
staging_songs_table_name = "staging_songs"
songplay_table_name = "songplays"
user_table_name = "users"
song_table_name = "songs"
artist_table_name = "artists"
time_table_name = "time"

# DROP TABLES
staging_events_table_drop = f"DROP TABLE IF EXISTS {staging_events_table_name};"
staging_songs_table_drop = f"DROP TABLE IF EXISTS {staging_songs_table_name};"
songplay_table_drop = f"DROP TABLE IF EXISTS {songplay_table_name};"
user_table_drop = f"DROP TABLE IF EXISTS {user_table_name};"
song_table_drop = f"DROP TABLE IF EXISTS {song_table_name};"
artist_table_drop = f"DROP TABLE IF EXISTS {artist_table_name};"
time_table_drop = f"DROP TABLE IF EXISTS {time_table_name};"

# CREATE TABLES
# diststyle even because the data is not skewed, even distribution is better
staging_events_table_create = (f"""
CREATE TABLE IF NOT EXISTS {staging_events_table_name} (
    artist VARCHAR(256),
    auth VARCHAR(256),
    firstName VARCHAR(256),
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR(256),
    length FLOAT,
    level VARCHAR(256),
    location VARCHAR(256),
    method VARCHAR(256),
    page VARCHAR(256),
    registration FLOAT,
    sessionId INT,
    song VARCHAR(256),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(256),
    userId INT
)
DISTSTYLE EVEN;
""")

# diststyle even because the data is not skewed, even distribution is better
staging_songs_table_create = (f"""
CREATE TABLE IF NOT EXISTS {staging_songs_table_name} (
    num_songs INT,
    artist_id VARCHAR(256),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(256),
    artist_name VARCHAR(256),
    song_id VARCHAR(256),
    title VARCHAR(256),
    duration DECIMAL,
    year INT
)
DISTSTYLE EVEN;
""")

# song_id is the distkey because it is used often in the join query
# start_time is the sortkey because it is used in the where clause
songplay_table_create = (f"""
CREATE TABLE IF NOT EXISTS {songplay_table_name} (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL SORTKEY,
    user_id INT NOT NULL,
    level VARCHAR(256),
    song_id VARCHAR(256) DISTKEY,
    artist_id VARCHAR(256),
    session_id INT,
    location VARCHAR(256),
    user_agent VARCHAR(256)
);
""")

# small table so distribute to all nodes
user_table_create = (f"""
CREATE TABLE IF NOT EXISTS {user_table_name} (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(256) NOT NULL,
    last_name VARCHAR(256) NOT NULL,
    gender CHAR(1),
    level VARCHAR(256)
)
DISTSTYLE ALL;
""")

# song_id is the distkey because it is used often in the join query
# year is the sortkey because it is used in the where clause
song_table_create = (f"""
CREATE TABLE IF NOT EXISTS {song_table_name} (
    song_id VARCHAR(256) PRIMARY KEY DISTKEY,
    title VARCHAR(256) NOT NULL,
    artist_id VARCHAR(256) NOT NULL,
    year INT SORTKEY,
    duration FLOAT
);
""")

# artist_id is the distkey because it is used often in the join query, but less than song_id
# name is the sortkey because it is used in the where clause
artist_table_create = (f"""
CREATE TABLE IF NOT EXISTS {artist_table_name} (
    artist_id VARCHAR(256) PRIMARY KEY DISTKEY,
    name VARCHAR(256) NOT NULL SORTKEY,
    location VARCHAR(256),
    latitude FLOAT,
    longitude FLOAT
);
""")

# start_time is the sortkey because it is used in the where clause
time_table_create = (f"""
CREATE TABLE IF NOT EXISTS {time_table_name} (
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday INT NOT NULL
)
DISTSTYLE ALL;
""")

# STAGING TABLES
staging_events_copy = (f"""
    copy {staging_events_table_name} from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={ARN}'
    json {log_json_path} compupdate off region 'us-west-2';
""")

staging_songs_copy = (f"""
    copy {staging_songs_table_name} from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={ARN}'
    json 'auto' compupdate off region 'us-west-2';
""")

# FINAL TABLES
songplay_table_insert = (f"""
INSERT INTO {songplay_table_name} (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    se.userId AS user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId AS session_id,
    se.location,
    se.userAgent AS user_agent
FROM {staging_events_table_name} se
JOIN {staging_songs_table_name} ss
ON se.song = ss.title AND se.artist = ss.artist_name
WHERE se.page = 'NextSong';
""")

user_table_insert = (f"""
INSERT INTO {user_table_name} (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    se.userId AS user_id,
    se.firstName AS first_name,
    se.lastName AS last_name,
    se.gender,
    se.level
FROM {staging_events_table_name} se
WHERE se.userId IS NOT NULL;
""")

song_table_insert = (f"""
INSERT INTO {song_table_name} (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    ss.song_id,
    ss.title,
    ss.artist_id,
    ss.year,
    ss.duration
FROM {staging_songs_table_name} ss
WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = (f"""
INSERT INTO {artist_table_name} (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    ss.artist_id,
    ss.artist_name AS name,
    ss.artist_location AS location,
    ss.artist_latitude AS latitude,
    ss.artist_longitude AS longitude
FROM {staging_songs_table_name} ss
WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = (f"""
INSERT INTO {time_table_name} (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(weekday FROM start_time) AS weekday
FROM {staging_events_table_name} se
WHERE se.ts IS NOT NULL;
""")


# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
