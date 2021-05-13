# Data Warehouse Project
- The task for this projects was to load data from S3 into a Redshift cluster using staging tables and then utilize these staging tables to insert data into new tables defined based on a predefined star schema. 

- The first step involved defining the queries that would drop and then create staging and other tables in our cluster.

- While creating the tables, I used the following distribution and sort keys:
| Table Name       | Sort Key     | Distribution Key      | Reasoning     |
| :------------- | :----------: | -----------: |:-----------: |
| songplay  | song_id   | start_time   |Sorted by song_id as it would be helpful to join with other tables and distributed using start_time as I expected the start_time to be evenly distibuted and since time table is present in all the nodes             |
| users     | user_id   |              |For analytics we'd be interested in seeing users and then activities so I decided to sort by user_id                 |
| songs     | song_id   |              |Since we will be frequently joining on songs to do analytics, I decided to sort by song_id              |
| artists   | artist_id |              |Since we will be needing artists for joining on, I decided to sort them by this column             |
| time      | start_time|              |Since it is a small dimension table, I have used 'ALL' distribution method              |

- For creating the cluser and IAM role, I have edited the dwh.cfg file to include configurations for the IAM role, cluser as well as AWS credentials. The codes used for this purpose are in the Cluster_IAM_and_Analytics.ipynb file. I have ran the two python scripts (create_tables.py and sql_queries.py) using the same Jupyter notebook.

- I also perfomed some analytics to see top 10 mostr active users, top 10 most popular songs, top 10 weeks-weekdays based on the count of sessions


