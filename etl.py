import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek


config = configparser.ConfigParser()
# Use credentials from .aws folder
config.read_file(open(f"{os.path.expanduser('~')}/.aws/credentials"))

# Set environment variables
os.environ["AWS_ACCESS_KEY_ID"]= config['default']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: Creates SparkSession object with specified config.

    Returns:
        SparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: Read songs data from JSON files stored on S3 and write songs
        and artists tables as parquet files on S3.

    Arguments:
        spark: SparkSession object 
        input_data: path to S3 bucket containing JSON files.
        output_data: path to S3 bucket where parquet files containing tables will be written

    Returns:
        None
    """
    
    # change AWS_DEFAULT_REGION to us-west-2 for read
    os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 
                             'year', 'duration']).distinct().where(
                             col('song_id').isNotNull())
    
    # change AWS_DEFAULT_REGION back to eu-central-1 for write
    os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'
    
    # write songs table to parquet files partitioned by year and artist
    songs_output = output_data + 'songs'
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(songs_output)

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 
                             'artist_latitude', 'artist_longitude']).distinct().where(
                             col('artist_id').isNotNull())
    artists_rename_exprs = ['artist_id as artist_id', 'artist_name as name',
                            'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude']
    artists_table = artists_table.selectExpr(*artists_rename_exprs)
    
    # write artists table to parquet files
    artists_output = output_data + 'artists'
    artists_table.write.mode('overwrite').parquet(artists_output)


def process_log_data(spark, input_data, output_data):
    """
    Description: Read log data from JSON files stored on S3 and write dim tables: time, users and fct
                 table: songplays

    Arguments:
        spark: SparkSession object 
        input_data: path to S3 bucket containing JSON files.
        output_data: path to S3 bucket where parquet files containing tables will be written

    Returns:
        None
    """
    # change AWS_DEFAULT_REGION to us-west-2 for read
    os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 
                             'gender', 'level']).distinct().where(
                             col('userId').isNotNull())
    
    # change AWS_DEFAULT_REGION back to eu-central-1 for write
    os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'
    
    # write users table to parquet files
    users_output = output_data + 'users'
    users_table.write.mode('overwrite').parquet(users_output)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(int(x)/1000))
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select(['start_time',
                          hour('start_time').alias('hour'),
                          dayofmonth('start_time').alias('day'),
                          weekofyear('start_time').alias('week'),
                          month('start_time').alias('month'),
                          year('start_time').alias('year'),
                          dayofweek('start_time').alias('weekday')]).distinct().where(
                              col('start_time').isNotNull())
    
    # write time table to parquet files partitioned by year and month
    time_output = output_data + 'time'
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(time_output)

    # change AWS_DEFAULT_REGION to us-west-2 for read
    os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'
    
    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    cond = [df.song == song_df.title, df.artist == song_df.artist_name, df.page == 'NextSong']
    songplays_table = df.join(song_df, cond).select(monotonically_increasing_id().alias('songplay_id'),
                                                    df.start_time,
                                                    df.userId.alias('user_id'),
                                                    df.level,
                                                    song_df.song_id,
                                                    song_df.artist_id,
                                                    df.sessionId.alias('session_id'),
                                                    song_df.artist_location.aliast('location'),
                                                    df.userAgent.alias('user_agent'))
    # change AWS_DEFAULT_REGION back to eu-central-1 for write
    os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'
    
    # write songplays table to parquet files partitioned by year and month
    songplays_output = output_data + 'songplays'
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(songplays_output)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://s3-5234624-emr-cluster/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
