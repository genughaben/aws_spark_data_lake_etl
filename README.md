# Documentation of a Spark ETL pipeline

## How to

**To execute locally on test data**:
* Extract log-data.zip and song_data.zip in data/
* Create a copy of dl_template.cfg and name it dl_local.cfg. You can leave all values as they are
* Open and run cells in etl.ipynb notebook

**To execute on AWS EMR cluster as Notebook**:
* Login to your AWS account.
* Create a Notebook in EMR with a Spark cluster. 3 machines m5.xlarge suffice. Make sure the cluster features at least Spark.
* Copy code from aws_cluster_notebook.ipynb into the notebook window.
* Fix output_data variable: add your notebooks or your own S3 bucket path!
* Run the jupyter notebook once the cluster is up.

**To execute on AWS EMR cluster as Script**:
* Login to your AWS account.
* Create a Notebook in EMR with a Spark cluster. 3 machines m5.xlarge suffice. Make sure the cluster features at least Spark.
* Once the cluster is up SSH into your cluster.
* SCP your project code onto the endpoint node.
* Create a copy of dl_template.cfg and name it dl.cfg. Adapt your credentials and your paths.
* Run etl.py. 

## Purpose

The music streaming startup Sparkify wants to move their grown data base from a date warehouse to a data lake. 
This project features ETF process to extract Sparkify's data from S3, compiles it with Spark into tables and l
loads the results into another S3 bucket comprised of a fact table [songplays] and the dimensional tables [songs, artists, users, time]. 
Thus Sparkify is able to perform analytics at scale to e.g. gain knowledge about their users listening behaviour.

## Database schema

The data is organised in a star schema consisting of the following tables:

**Fact table**

**1. songplays:** records in log data associated with song plays i.e. records with page NextSong

*Columns:* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

##### Dimension Tables

**2. users** - users in the app

*Columns*: user_id, first_name, last_name, gender, level

**3. songs** - songs in music database

*Columns*: song_id, title, artist_id, year, duration

**4. artists** - artists in music database

*Columns*: artist_id, name, location, lattitude, longitude

**5. time** - timestamps of records in songplays broken down into specific units

*Columns*: start_time, hour, day, week, month, year, weekday
 
## Files and ETL process description

**Files**
* *dl_template.cfg* - A template. Create a copy and adapt values for cluster use.
* *etl.ipynb* - A jupyter notebook for local use.
* *aws_cluster_notebook.ipynb* - A jupyter notebook for AWS EMR cluster Notebook use.
* *data/* - Folder containing test data for local use.
* *etl.py* - A python script for running ETL on a AWS EMR cluster.

**ETL process**

The ETL process has two parts.
1. process_song_data: song_data is read and song and artist tables are created. The results are writen to parquet files.
2. process_log_data: log_data that contains songplay event data in rows where page columns has value "NextSong" is read 
and the tables users, time and songplays are created from the data in the rows. 
