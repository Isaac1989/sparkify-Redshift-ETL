import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE  IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= """
CREATE TABLE IF NOT EXISTS stage_events (
artist          VARCHAR,
auth            VARCHAR, 
firstName       VARCHAR,
gender          VARCHAR,   
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR, 
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS stage_songs ( 
song_id            VARCHAR,
num_songs          INTEGER,
title              VARCHAR,
artist_name        VARCHAR,
artist_latitude    FLOAT,
year               INTEGER,
duration           FLOAT,
artist_id          VARCHAR,
artist_longitude   FLOAT,
artist_location    VARCHAR
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS songplay(
songplay_id                      INT IDENTITY(0,1)           PRIMARY KEY sortkey,
starttime                        TIMESTAMP                   NOT NULL,
user_id                          INTEGER                     NOT NULL,
level                            VARCHAR                      ,
song_id                          VARCHAR                     NOT NULL,
artist_id                        VARCHAR                     NOT NULL,
session_id                       INTEGER                     NOT NULL,
location                         VARCHAR                      ,
user_agent                       VARCHAR                      ,
FOREIGN KEY (user_id) REFERENCES users (user_id),
FOREIGN KEY (song_id) REFERENCES song (song_id),
FOREIGN KEY (artist_id) REFERENCES artist (artist_id),
FOREIGN KEY (starttime) REFERENCES times (starttime)
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS users (
user_id                        INTEGER        PRIMARY KEY,
first_name                     VARCHAR        ,
last_name                      VARCHAR        ,
gender                         VARCHAR        ,
level                          VARCHAR
)
diststyle all;
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS  song(
song_id                            VARCHAR                    PRIMARY KEY,
title                              VARCHAR                     ,
artist_id                          VARCHAR                     ,
year                               INTEGER                     ,
duration                           FLOAT            
)
diststyle all;
"""
artist_table_create = """
CREATE TABLE IF NOT EXISTS artist (
artist_id                VARCHAR                    PRIMARY KEY,
name                     VARCHAR                      ,
location                 VARCHAR                      ,
latitude                 FLOAT                        ,
longitude                FLOAT
)
diststyle all;
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS times (
starttime              TIMESTAMP            PRIMARY KEY,
hour                   SMALLINT             ,
day                    SMALLINT             ,
week                   SMALLINT             ,
month                  SMALLINT             ,
year                   SMALLINT             ,
weekday                SMALLINT             
)
diststyle all;
"""

# STAGING TABLES

staging_events_copy = """
    COPY stage_events FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON '{}';
""".format(config.get('S3','LOG_DATA'),
           config.get('IAM_ROLE', 'ARN'), 
           config.get('S3','LOG_JSONPATH'))

staging_songs_copy = """
    COPY stage_songs FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto' 
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""".format(config.get('S3', 'SONG_DATA'),
           config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplay  (starttime, user_id, level, song_id, artist_id, session_id, location, user_agent
)
SELECT 
    DISTINCT e.ts,
    e.userId,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId,
    e.location,
    e.userAgent
FROM stage_events e
LEFT JOIN stage_songs s
ON  e.artist = s.artist_name 
AND e.song = s.title
AND e.length = s.duration
WHERE e.userId IS NOT NULL
AND e.page='NextSong'
AND s.song_id IS NOT NULL;
"""

user_table_insert = """
INSERT INTO users (user_id, first_name, last_name, gender, level
)
SELECT
    DISTINCT userId,
    firstName,
    lastName,
    gender,
    level
FROM stage_events
WHERE userId IS NOT NULL
AND page='NextSong';
"""

song_table_insert = """
INSERT INTO song (song_id, title, artist_id, year, duration
)
SELECT 
    DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM stage_songs
WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
INSERT INTO artist (artist_id, name, location, latitude, longitude
)
SELECT
    DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM stage_songs
WHERE artist_id IS NOT NULL;
"""

time_table_insert = """
INSERT INTO times (starttime, hour, day, week, month, year, weekday)
SELECT 
                DISTINCT ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
FROM stage_events
WHERE ts IS NOT NULL;
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,
                       songplay_table_insert]
