import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
# Use credentials from .aws folder
config.read_file(open(f"{os.path.expanduser('~')}/.aws/credentials"))

# Set environment variables
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: Creates SparkSession object with specified config.

    Returns:
        SparkSession object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
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
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_columns = ['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = df.select(songs_columns).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_output = output_data + 'songs'
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(songs_output)

    # extract columns to create artists table
    artists_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = df.select(artists_columns).dropDuplicates()
    artists_rename_exprs = ['artist_id as artist_id', 'artist_name as name',
                            'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude']
    artists_table = artists_table.selectExpr(*artists_rename_exprs)
    
    # write artists table to parquet files
    artists_output = output_data + 'artists'
    artists_table.write.mode('overwrite').parquet(artists_output)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =

    # read log data file
    df = 
    
    # filter by actions for song plays
    df = 

    # extract columns for users table    
    artists_table = 
    
    # write users table to parquet files
    artists_table

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = 
    
    # write time table to parquet files partitioned by year and month
    time_table

    # read in song data to use for songplays table
    song_df = 

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = 

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
