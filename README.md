# udacity-dend-project-3: Data Warehouse

### Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


### Data Model
**Star Schema** data model - 1 Fact Table and 4 Dimension Tables:

![ERD](https://github.com/jamesli0512/udacity-dend-project-3/blob/master/ERD.JPG)

#### Fact Table:
**songplays_fact** - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables:

**users_dim** - users in the app
user_id, first_name, last_name, gender, level

**songs_dim** - songs in music database
song_id, title, artist_id, year, duration

**artist_dim** - artists in music database
artist_id, name, location, lattitude, longitude

**time_dim** - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday
