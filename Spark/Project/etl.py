import configparser
from datetime import datetime
import os
import calendar
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['default']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():

    """
    Description: This function  creates or get (if already exists) a Spark session 
    
    Arguments:
        None

    Returns:
        spark: Spark session
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.4") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    """
    Description: This function process songs data and write songs and artists tables to parquet files
        
    Arguments:
        spark: Spark session
        input_data: input files repository path
        output_data: output files repository path
    Returns:
        None
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/*/*/*.json")

    # read song data file
    print ("    Reading song data file") 
    df = spark.read.json(song_data)

    # write songs table to parquet files partitioned by year and artist
    print ("    Writing songs table to parquet files")
    
    df.drop_duplicates(subset=['song_id']).select("song_id", "title", "artist_id", "year", "duration") \
        .write.mode("overwrite") \
        .partitionBy("year","artist_id") \
        .parquet(output_data + "songs")

    # write artists table to parquet files
    print ("    Writing artists table to parquet files")

    df.drop_duplicates(subset=['artist_id']).select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude") \
            .write.mode("overwrite") \
            .parquet(output_data + "artists")
        
    df.createOrReplaceTempView("songsData")


def process_log_data(spark, input_data, output_data):
    
    """
    Description: This function process logs data and write users, time and songplays tables to parquet files
        
    Arguments:
        spark: Spark session
        input_data: input files repository path
        output_data: output files repository path
    Returns:
        None
    """

    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    print ("    Reading log data file") 
    df = spark.read.json(log_data)
    
    # write users table to parquet files
    print ("    Writing users table to parquet files")

    df.drop_duplicates(subset=['userId']).select(df.userId.alias("user_id"), df.firstName.alias("first_name") , \
        df.lastName.alias("last_name"), "gender", "level") \
        .write.mode("overwrite") \
        .parquet(output_data + "users")

    # create timestamp column from original timestamp column
    get_timestamp = f.udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column

    get_week = f.udf(lambda x: calendar.day_name[x.weekday()])
    get_weekday = f.udf(lambda x: x.isocalendar()[1])
    get_hour = f.udf(lambda x: x.hour)
    get_day = f.udf(lambda x : x.day)
    get_year = f.udf(lambda x: x.year)
    get_month = f.udf(lambda x: x.month)

    # extract columns to create time table
    df = df.withColumn('start_time', (df['ts']/1000).cast('timestamp'))
    df = df.withColumn('hour', get_hour(df.start_time))
    df = df.withColumn('day', get_day(df.start_time))
    df = df.withColumn('week', get_week(df.start_time))
    df = df.withColumn('month', get_month(df.start_time))
    df = df.withColumn('year', get_year(df.start_time))
    df = df.withColumn('weekday', get_weekday(df.start_time))

    # write time table to parquet files partitioned by year and month
    print ("    Writing time table to parquet files")
    df.drop_duplicates(subset=['start_time']).select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday') \
        .write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_data + "time")
    
    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM songsData")

    # filter by actions for song plays
    dfNextSong = df.filter(df.page == "NextSong")

    # extract columns from joined song and log datasets to create songplays table
    print ("    Writing songplays table to parquet files")
    dfNextSong.join(song_df, song_df.artist_name == df.artist, "inner") \
        .distinct() \
        .select(f.col("start_time"), f.col("userId"), f.col("level"), f.col("sessionId"), \
                f.col("location"), f.col("userAgent"), f.col("song_id"), f.col("artist_id"), \
                df['year'].alias('year'), df['month'].alias('month')) \
        .withColumn("songplay_id", f.monotonically_increasing_id()) \
        .write.mode("overwrite") \
        .partitionBy('year', 'month') \
        .parquet(output_data + "songplays")


def main():
    print("BEGINNING")
    print ("1. Creating/Getting Spark session")
    start_time = time.time()
    spark = create_spark_session()
    print("--- It took %s seconds ---" % (time.time() - start_time))

    # S3 buckets
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://pg-west2-udacity/parquets/"

    # Local for dev purpose
    #input_data = "data/"
    #output_data = "data/parquets/"

    start_time = time.time()
    print ("2. Starting SONG data processing")    
    process_song_data(spark, input_data, output_data)
    print("--- It took %s seconds ---" % (time.time() - start_time))

    start_time = time.time()
    print ("3. Starting LOG data processing")
    process_log_data(spark, input_data, output_data)
    print("--- It took %s seconds ---" % (time.time() - start_time))
    print("END")

if __name__ == "__main__":
    main()