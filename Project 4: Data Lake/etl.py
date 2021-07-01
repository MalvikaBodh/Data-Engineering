import configparser
import os
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as F
from pyspark.sql.functions import dayofweek
from pyspark.sql.types import DateType
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
     """
    Description: Function loads json song data from S3 bucket processes it into song and artist table and writes the parquet output to S3 bucket 
        
    Parameters spark: Specifies the spark session returned from create_spark_session function
          input_data: Location of the s3 bucket where our input data is stored
         output_data: Location of the s3 bucket where we will write the parquet files
    """
    # get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/*/*/*/*.json')
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(output_data+'/songs/', mode="overwrite")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude","artist_longitude").dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'/artists/', mode="overwrite")


def process_log_data(spark, input_data, output_data):
         """
    Description: Function loads json log data from S3 bucket processes it into user and time tables. Reads in song data parquet files, combines data from
    song data, log data and time tables to create a final songplay table and finally writes the parquet output to S3 bucket 
        
    Parameters spark: Specifies the spark session returned from create_spark_session function
          input_data: Location of the s3 bucket where our input data is stored
         output_data: Location of the s3 bucket where we will write the parquet files
    """
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df= df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level" ).dropDuplicates().filter(df["UserId"] != "")
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'/users/', mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x:datetime.datetime.fromtimestamp(x/ 1e3), TimestampType())
    df =  df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x/ 1e3), DateType())
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # extract columns to create time table

    time_table = df.withColumn("start_time", date_format(col("timestamp"), "HH:mm:ss")).select("start_time",  hour("timestamp").alias('hour'),\
                                                                             dayofmonth("timestamp").alias('day'),\
                                                                             F.date_format("timestamp", "W").alias('week'),\
                                                                             month("timestamp").alias('month'),\
                                                                             year("timestamp").alias('year'),\
                                                                             F.dayofweek("timestamp").alias('weekday')\
                                                                             )
    
    # write time table to parquet files partitioned by year and month
    time_table = time_table.dropDuplicates(['start_time'])
    time_table.write.partitionBy("year","month").parquet(output_data+'/time_table/', mode="overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'/songs')
    song_df.createOrReplaceTempView("song_df")

    # extract columns from joined song and log datasets to create songplays table 
    df = df.withColumn("start_time", date_format(col("timestamp"), "HH:mm:ss"))
    df.createOrReplaceTempView("df")
    time_table.createOrReplaceTempView("time_table")
    songplays_table = spark.sql("""select row_number() over (order by df.ts) as songplay_id, 
        df.start_time, df.userId, df.level , song_df.song_id, song_df.artist_id, df.sessionId, 
        df.location, df.userAgent, time_table.year, time_table.month  
        from df 
            left join song_df 
        on df.song = song_df.title
            left join time_table
        on df.start_time = time_table.start_time        
        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+'/songplays/', mode="overwrite")


def main():
             """
    This function will first create the spark session then run the processing of song and log data, will convert 
    them into dimensional tables and store the outputs in the form of partitioned/non-partitioned parquet files 
    """

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://project-datalake5mlvk"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
