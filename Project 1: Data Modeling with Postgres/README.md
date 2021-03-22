# Data Modeling with Postgres  

## Overview
In this project I have built an ETL pipeline using Postgres database with tables designed to optimize queries on song play analysis for a startup called Sparkify. 

They want to analyze the data they've been collecting on songs and user activity (in the form of *JSON* logs) on their new music streaming app.

## Project
###  PART 1. SQL scripts
The first topic of focus in the project is to write the script for queries that will:
the tables in Postgres for data insertion. As such, I first wrote the script for creating tables and defining specific constraints and primary keys, foreign keys etc. for the tables along with the queries for insertion of data into the tables. 


1. Drop tables if they exist in the database.
2. Create the 5 tables that we will define to hold the transformed JSON logs and songs data.
3. Insert data into these newly defined tables.
 


#### The new tables with definition are: 

-  User Table (users)
    
   
| Column Name      | Data Type | Constraint|
| :----------- | :----------- |----------- |
| user_id      | int       | PRIMARY KEY       |
| first_name   | varchar        |        |
| last_name   | varchar        |        |
| gender   | varchar        |        |
| level   | varchar        | NOT NULL      |

-  Songs Table (songs)
    
   
| Column Name      | Data Type | Constraint|
| ----------- | ----------- |----------- |
| song_id      | varchar       | PRIMARY KEY       |
| title   | varchar        |  NOT NULL      |
| artist_id   | varchar        | NOT NULL       |
| year   | int        |     NOT NULL   |
| duration   | float        | NOT NULL      |

-  Artists Table (artists)
    
   
| Column Name      | Data Type | Constraint|
| ----------- | ----------- |----------- |
| artist_id      | varchar       | PRIMARY KEY       |
| name   | varchar        |  NOT NULL      |
| location   | varchar        | NOT NULL       |
| latitude   | numeric        |     |
| longitude   | numeric        |     |

- Time Table (time)
    
   
| Column Name      | Data Type | Constraint|
| ----------- | ----------- |----------- |
| start_time      | timestamp       | PRIMARY KEY       |
| hour   | int        |  NOT NULL      |
| day   | int        | NOT NULL       |
| week   | int        |  NOT NULL    |
| month   | int        |  NOT NULL    |
| year   | int        |   NOT NULL   |
| weekday   | int        |  NOT NULL    |

- Song Play Table (songplay)
    
   
| Column Name      | Data Type | Constraint|
| ----------- | ----------- |----------- |
| songplay_id      | serial       | PRIMARY KEY       |
| start_time   | time_stamp        |foreign key (start_time) REFERENCES time (start_time)     |
| user_id   | int        | NOT NULL and foreign key (user_id) REFERENCES users (user_id)      |
| level   | varchar        |  NOT NULL    |
| song_id   | varchar        |  foreign key (song_id) REFERENCES songs (song_id)    |
| artist_id   | varchar        | foreign key (artist_id) REFERENCES artists (artist_id)     |
| session_id   | int        |  NOT NULL    |
| location   | varchar        |       |
| user_agent   | text        |      |


+ The values are then inserted into these tables. Since, there can be duplicated user accounts between level, I am overwriting their level (free/paid) with new insertion. 
+ For songs, atrists and time, in case of conflicts while insertion we are setting the insert query to do nothing 
+ Running create_tables.py in the terminal will create a new sparkify database, drop any tables and then create the five tables


### PART 2. TEST TABLES using etl.ipynb 


The second part of the project was to check our newly created queries and see if they are perfoming their job well. We load data from json logs and songs data file and then try out the pipeline with only a few records to troublsehoot any error in our queries. 
   
After I was satisfied with the queries, I used the test.ipynb file to check if the tables were created and loaded as expected. 
    
### PART 3. Executing etl.py 

In this section, we use etl.py to first read all of the files in the data folder and then create songplays table which an be used to query information required by the analytics teams at Sparkify. Since log files do not have song_id and artist_id, we perform a process to join on these two id's, so now we have a sonplay table that can be used to perform analytics and any other dimension information can be joined using the dimesion tables (user, artists, songs, time).
 