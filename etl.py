import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

# AWS header
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

"""
Load song from S3, 
extract as song_table, artists_table,
save to S3
"""
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    #Local Testing
    #song_data = os.path.join(input_data, 'song_data_test.json')

    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title', 'artist_id', 'year','duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs_table/')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 
                              col('artist_name').alias('name')
                             ,col('artist_location').alias('location')
                             ,col('artist_latitude').alias('latitude')
                             ,col('artist_longitude').alias('longitude')).dropDuplicates()
    
    # write artists table to parquet files
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, "songs/songs.parquet"), 'overwrite')

"""
Process Log Data
    
"""
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')
    #Local Testing
    #log_data = os.path.join(input_data, 'log_data_test.json')


    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level') \
                    .dropDuplicates() \
                    .withColumnRenamed('userId', 'user_id') \
                    .withColumnRenamed('firstName', 'first_name') \
                    .withColumnRenamed('lastName', 'last_name')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime') \
        .withColumn('start_time', col('datetime')) \
        .withColumn('hour', hour('datetime')) \
        .withColumn('day', dayofmonth('datetime')) \
        .withColumn('week', weekofyear('datetime')) \
        .withColumn('month', month('datetime')) \
        .withColumn('year', year('datetime')) \
        .withColumn('weekday', dayofweek('datetime')) \
        .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time/time.parquet"), 'overwrite')


    # read in song data to use for songplays table
    song_df = spark.read.json(os.path.join(input_data, "song_data/*/*/*/*.json"))
    #Local Testing
    #song_df = spark.read.json(os.path.join(input_data, "song_data_test.json"))


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name) & (df.length == song_df.duration), 'left_outer') \
                        .select(
                            col("timestamp").alias("start_time"),
                            col("userId").alias("user_id"),
                            df.level,
                            song_df.song_id,
                            song_df.artist_id,
                            col("sessionId").alias("session_id"),
                            df.location,
                            col("userAgent").alias("user_agent"),
                            year('datetime').alias('year'),
                            month('datetime').alias('month')
                        ).withColumn("songplay_id", monotonically_increasing_id())
                            

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, "songplays/songplays.parquet"), 'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-p4-datalakes/"
    
    #Local Testing
    #input_data = "./data/test/"
    #output_data = "./udacity-p4-datalakes/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
