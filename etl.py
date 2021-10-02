import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    """
    Create a Spark session to process the data.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load song_data from input_data path, process the data to extract song_table and artists_table, 
    and store the queried data to parquet file.
        spark  : The Spark session that will be used to execute commands.
        input_data : The input data to be processed.
        output_data : The location where to store the parquet tables.
    """
    # get filepath to song data file
    song_data = input_data_s
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    # Drop rows with duplicate song_ids
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]) \
        .dropDuplicates(['song_id'])
    number_par = songs_table.rdd.getNumPartitions()
    
    songs_table.printSchema()
    songs_table.show(5, truncate=False)
    
    # write songs table to parquet files partitioned by year and artist
    song_table.write.parquet(output_data + 'songs_table', 
                                partitionBy=['year', 'artist_id'], 
                                mode='Overwrite')

    # extract columns to create artists table
    # Drop rows with duplicate artist_ids
    artists_table = df.select(
        df.artist_id,
        F.col("artist_name").alias("name"),
        F.col("artist_location").alias("location"),
        F.col("artist_latitude").alias("latitude"),
        F.col("artist_longitude").alias("longitude")
        ).dropDuplicates(['artist_id'])
        
    number_par = artists_table.rdd.getNumPartitions()
    
    artists_table.printSchema()
    artists_table.show(5, truncate=False)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table', mode='Overwrite')

    return df

def process_log_data(spark, input_data, output_data):
    """
        Load log_data from input_data path,
        process the data to extract users_table, time_table,
        songplays_table, and store the queried data to parquet files.
           Parameters:
        spark  : The Spark session that will be used to execute commands.
        input_data : The input data to be processed.
        output_data : The location where to store the parquet tables.
    """
    # get filepath to log data file
    log_data = input_data_l

    # read log data file
    df = spark.read.json(input_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table  
    # remove duplicate rows
    users_table = df.select(
        F.col("userId").alias('user_id').cast(T.LongType()),
        F.col("firstName").alias('first_name'),
        F.col("lastName").alias('last_name'),
        df.gender,
        df.level,
    ).dropDuplicates(['user_id'])
    
    number_par = users_table.rdd.getNumPartitions()
    
    users_table.printSchema()
    users_table.show(5)
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table', mode='Overwrite')

    # define functions for extracting time components from ts field
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    
    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # extract columns to create time table
    # remove duplicate rows

    time_table = df.select(F.col("start_time").cast(T.TimestampType()),
                           F.hour("start_time").alias("hour"),
                           F.dayofmonth("start_time").alias("day"),
                           F.weekofyear("start_time").alias("week"),
                           F.month("start_time").alias("month"),
                           F.year("start_time").alias("year"),
                           F.dayofweek("start_time").alias("weekday")) \
        .dropDuplicates(['start_time'])
    number_par = time_table.rdd.getNumPartitions()
    # write time table to parquet files partitioned by year and month
   
                        
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time_table', partitionBy=['year', 'month'], mode='Overwrite')
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    # First step: Join by song title and artist name
    # Second step: select the appropriate columns from each
    # where df refer to the logs df, and songs_df refer to the songs data df
    songplays_table = df.join(songs_df,
                              (df.song == songs_df.title) &
                              (df.artist == songs_df.artist_name)) \
        .select(
            F.monotonically_increasing_id().alias("songplay_id"),
            F.col("start_time").cast(T.TimestampType()),
            F.col("userId").alias('user_id').cast(T.LongType()),
            df.level,
            songs_df.song_id,
            songs_df.artist_id,
            F.col("sessionId").alias("session_id"),
            df.location,
            F.col("useragent").alias("user_agent")
    )

    number_par = songplays_table.rdd.getNumPartitions()

    # write songplays table to parquet files partitioned by year and month
    songplay_table_df.write.parquet(output_data + 'songplays_table', 
                                    partitionBy=['year', 'month'], 
                                    mode='Overwrite')


def main():

    spark = create_spark_session()
    input_data_s = "s3://udacity-dend/song_data/*/*/*/*.json"
    input_data_l = "s3://udacity-dend/log_data/*/*/*.json"  
    output_data = "s3://bucket_name/"

    process_song_data(spark, input_data_s, output_data)    
    process_log_data(spark, input_data_l, output_data)


if __name__ == "__main__":
    main()
