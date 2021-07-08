# Project No. 5 - Data Pipeline 

In this project, we are creating custom operators and using them in our Apache Airflow DAG to introduce more automation and monitoring to Sparkify's data warehouse ETL pipelines 

The source data resides in S3 and will be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

#### Their data was stored in S3 under two directories, one for song metadata and another for user activity log

- song_data : s3a://udacity-dend/song_data
- log_data : s3a://udacity-dend/log_data

#### Output created as a result of this project and stored in Amazon Redshift
- songs table : unqiue songs present in Sparkify's data, partitioned by year and artist_id
- artists table : unique artists present in the songs metadata directory
- users table : table with unique users on Sparkify's app
- time table : with unique start times and their corresponding values for year, month, week etc.
- songplays table: records in log data associated with song plays, partitioned by year and month

#### Project Files
The project consists of 2 main directories

1. dags :
		- Has the python file udac_example_dag.py that runs our dag 
        - SQL script for creating all our tables in Redshift

2. plugins : which has the entire script for etl. This reads data from s3 buckets, processes the data, and then writes the final dimesnional table out puts to s3
    -  Helpers has a python file (sql_queries.py) that describes the insert statement or queries that will be needed for loading data into the tablessql
    -  Operators has 4 python files that have custom operators defined in them
        1. stage_redshift.py this custom operator allows us to copy data from S3 to Redshift
        2. load_fact.py this custom operator allows us to load fact tables
        3. load_dimensions this customer operator allows us to load data into dimension tables
        4. data_quality.py this customer operator allows us to perform data quality check in out DAG

##### DAG Created as a result of the project
![image info](./Capture.png)