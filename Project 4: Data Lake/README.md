# Project 4 Data Lake (Sparkify)
In this project, we were tasked with shifting Sparkify's data warehouse to a datalake since the startups had outgrown their user base and song database even more. Their data was available in S3, with JSON logs on user activity, as well as JSON metadata on the songs in their App.

Their data was stored in S3 under two directories, one for song metadata and another for user activity log    
   - song_data : s3a://udacity-dend/song_data
   - log_data  : s3a://udacity-dend/log_data

#### Output created as a result of this project and stored in directories on an S3 bucket #

   - songs table : unqiue songs present in Sparkify's data, partitioned by year and artist_id
   - artists table : unique artists present in the songs metadata directory
   - users table : table with unique users on Sparkify's app
   - time table : with unique start times and their corresponding values for year, month, week etc.
   - songplays table: records in log data associated with song plays, partitioned by year and month
   
#### Project Files
The project consists of 2 main files. 
1. dl.cfg :  which has the AWS credentials for the user used to create the spark session as well as run other processes

2. etl.py :  which has the entire script for etl. This reads data from s3 buckets, processes the data, and then writes the final dimesnional table out puts to s3

#### How was project implemented?

1. Created an IAM Role with the following access (AmazonS3FullAccess, AdministratorAccess, AmazonEC2FullAccess)

2. Configured the git bash shell using (aws confiugure) 

3. Created s3 bucket for exporting processed data in the us-west-2 region
           aws s3api create-bucket 
           --bucket project-datalake5mlvk 
           --region us-west-2 
           --create-bucket-configuration LocationConstraint=us-west-2

4. Created a subnet group in us-west-2 region and edited inbound rules to accept ssh at port 22

5. Created an EMR cluster in us-west-2 region and replaced the subnet from prior step below
        aws emr create-cluster \
        --name datalakeprojectfinal \
        --use-default-roles \
        --release-label emr-5.28.0 \
        --instance-count 3 \
        --applications Name=Spark  \
        --ec2-attributes KeyName=datalakeproject,SubnetId= subnetidname\
         --instance-type m5.xlarge \
        --region us-west-2 \
        --profile profilename
    
6. Copied the local etl and configuration file over to hadoop home   
          scp -i  datalakeproject.pem etl.py hadoop@ec-2masteraddress:/home/hadoop/
          scp -i  datalakeproject.pem dl.cfg hadoop@ec-2masteraddress:/home/hadoop/

7. Logged into the EMR cluser using
          ssh -i datalakeproject.pem hadoop@ec-2masteraddress
          
8. Ran the following to fix the config parser no module error
        sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh

9. Ran the ETL scrip on Hadoop cluster using the following 
         spark-submit etl.py
         
10. Verified the job results using spark WebUI and confirmed the creation of 5 tables in the s3 bucket