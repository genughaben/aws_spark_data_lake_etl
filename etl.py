import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, trim
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import hour, dayofmonth, weekofyear, month, year, dayofweek
from pyspark.sql.types import TimestampType


def create_spark_session():
    '''Creates a Spark session.

    Output:
    * spark -- Spark session.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def log(df, tab='', msg='', verbose=True):
    ''' Prints schema and count of df as well as a optional message if verbose is True.

    Keyword arguments:
    * df            -- Spark dataframe.
    * tab           -- string: table name
    * msg           -- string: optional message
    * verbose       -- boolean
    '''

    if verbose:
        if msg != '':
            print(msg)
        df.printSchema()
        print(f'count({tab}): {df.count()}')

def process_song_data(spark, input_data, output_data, verbose=False):    
    ''' Compile song json input data from input_data path into:
        song_data and artist_data schema and saving these as 
        parquet files.
        
    Keyword arguments:
    * spark         -- Spark session.
    * input_data    -- path to input_data
    * output_data   -- path to save parquet output files
    * verbose       -- bool to display logs if True
    '''
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    log(df, 'song_input', verbose=verbose)

    # extract columns to create songs table
    # schema: song_id, title, artist_id, year, duration
    song_schema = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.filter(df.song_id != '')\
                    .dropDuplicates(['song_id'])\
                    .withColumn('title', trim(df.title)) \
                    .select(song_schema)\
                    .orderBy('song_id')

    log(songs_table, 'song_output', verbose=verbose)
    
    # write songs table to parquet files partitioned by year and artist
    song_data_output = os.path.join(output_data, 'songs')
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(song_data_output)
    
    # extract columns to create artists table
    # artist_id, name, location, lattitude, longitude
    # artist_schema = ['artist_id', 'name', 'location', 'latitude', 'longitude']
    artists_table = df.filter(df.artist_id != '')\
                      .dropDuplicates(['artist_id'])\
                      .withColumn('artist_name', trim(df.artist_name))\
                      .select(
                        col('artist_id'),
                        col('artist_name').alias('name'),
                        col('artist_location').alias('location'),
                        col('artist_latitude').alias('latitude'),
                        col('artist_longitude').alias('longitude')
                       )

    log(artists_table, 'artists_table', verbose=verbose)
        
    # write artists table to parquet files
    artists_data_output = os.path.join(output_data, 'artists')
    artists_table.write.mode('overwrite').parquet(artists_data_output)


def process_log_data(spark, input_data, output_data, verbose=False):
    ''' Compile event log json input data from input_data path into:
    users, time and songplays table schema and saving these as 
    parquet files.
        
    Keyword arguments:
    * spark         -- Spark session.
    * input_data    -- path to input_data
    * output_data   -- path to save parquet output files
    * verbose       -- bool to display logs if True
    '''

    # get filepath to log data file
    log_data = os.path.join(input_data, 'log-data/*.json')

    # read log data file
    df = spark.read.json(log_data)
  
    log(df, 'log', verbose=verbose)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # extract columns for users table
    # user_columns = ['user_id', 'first_name', 'last_name', 'gender', 'level']
    users_table = df.filter(df.userId != '')\
                      .dropDuplicates(['userId'])\
                      .withColumn('firstName', trim(df.firstName))\
                      .withColumn('lastName', trim(df.lastName))\
                      .select(
                        col('userId').alias('user_id'),
                        col('firstName').alias('first_name'),
                        col('lastName').alias('last_name'),
                        col('gender'),
                        col('level')                    
                    )
    
    log(users_table, 'users', verbose=verbose)
    
    users_data_output = os.path.join(output_data, 'users')
    users_table.write.mode('overwrite').parquet(users_data_output)
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn('time', get_timestamp(df.ts))

    df = df.withColumn('start_time', col('ts'))\
           .withColumn('hour', hour(df.time))\
           .withColumn('day', dayofmonth(df.time))\
           .withColumn('week', weekofyear(df.time))\
           .withColumn('month', month(df.time))\
           .withColumn('year', year(df.time))\
           .withColumn('weekday', dayofweek(df.time))
    
    time_table = df.dropDuplicates(['ts']).orderBy('ts')
    
    time_table.createOrReplaceTempView('time_table')
    time_table = spark.sql('''
        SELECT DISTINCT start_time, hour, day, week, month, year, weekday 
        FROM time_table
    ''')
    
    log(time_table, 'time_table', verbose=verbose)    
    
    # write time table to parquet files partitioned by year and month
    time_data_output = os.path.join(output_data, 'time')
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(time_data_output)

    # read in song data to use for songplays table
    song_path = os.path.join(output_data, 'songs')
    song_df = spark.read.parquet(song_path)
    
    # extract columns from joined song and log datasets to create songplays table 
    # songplay_cols = ['songplay_id', 'start_time', 'user_id', 'level', 'song_id', \
    #                  'artist_id', 'session_id', 'location', 'user_agent']

    df.withColumn('song', trim(df.song))
    df = df.alias('log').join(song_df.alias('song'),col('song.title') == col('log.song'))

    songplays_table = df.withColumn('songplay_id', monotonically_increasing_id())\
                        .select(
                            col('ts').alias('start_time'),
                            col('userId').alias('user_id'),
                            'level',
                            'song_id',
                            'artist_id',
                            col('sessionId').alias('session_id'),
                            'location',
                            col('userAgent').alias('user_agent'),
                            'log.year',
                            'log.month'
                        )

    log(songplays_table, 'songplays', verbose=verbose)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_output_path = os.path.join(output_data, 'songplays')
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(songplays_output_path)


def main():
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    input_data = config['PATH']['INPUT_DATA']
    output_data = config['PATH']['OUTPUT_DATA']

    spark = create_spark_session()
    # input_data = "s3a://udacity-dend/"
    # output_data = ""
    #
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
