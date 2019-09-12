# Spark ETL pipeline for Sparkify

## How to



##Purpose

The music streaming startup Sparkify wants to move their grown data base from a date warehouse to a data lake. 
This project features ETF process to extract Sparkify's data from S3, compiles it with Spark into tables and l
loads the results into another S3 bucket comprised of a fact table [songplays] and the dimensional tables [songs, artists, users, time]. 
Thus Sparkify is able to perform analytics at scale to e.g. gain knowledge about their users listening behaviour.

##Database schema

The data is organised in a star schema consisting of the following tables:

**Fact table**

**1. songplays:** records in log data associated with song plays i.e. records with page NextSong

*Columns:* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#####Dimension Tables

**2. users** - users in the app

*Columns*: user_id, first_name, last_name, gender, level

**3. songs** - songs in music database

*Columns*: song_id, title, artist_id, year, duration

**4. artists** - artists in music database

*Columns*: artist_id, name, location, lattitude, longitude

**5. time** - timestamps of records in songplays broken down into specific units

*Columns*: start_time, hour, day, week, month, year, weekday
 
##Files and ETL process description

**Files**
* dl_template.cfg

The ETL process has two parts.