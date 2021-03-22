import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function reads all the data from filepath and inserts it into artists and songs table
    Arguments:
        cur: the cursor object.
        filepath: song data file path.

    Returns:
        None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    for index, row in df.iterrows():
        song_data = (row.song_id, row.title, row.artist_id, row.year, row.duration)
        cur.execute(song_table_insert, song_data)
    
    # insert artist record
    
    for index, row in df.iterrows():
        artist_data = (row.artist_id, row.artist_name, row.artist_location, row.artist_latitude, row.artist_longitude)
        cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: This function is responsible for processing data from the logs data directory into tables after performing a series of data enhancemments and data
    transformations. As a final step, data is inserted into songplay table by using the song select query created in sqk_queries.py. The select query iterates over 
    index in the dataset and selects the required columns.

    Arguments:
        cur: the cursor object.
        filepath: log data file path.
       
    Returns:
        None
    """
    # open log file
    
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong'].reset_index(drop=True)

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit ='ms')
    df['hour'] = df['ts'].dt.hour
    df['day'] = df['ts'].dt.day
    df['week'] = df['ts'].dt.week
    df['month'] = df['ts'].dt.month
    df['year'] = df['ts'].dt.year
    df['weekday'] = df['ts'].dt.weekday
    # insert time data records
    time_data = df[['ts','hour','day','week','month','year','weekday']]
    column_labels = ['start_time','hour','day','week','month','year','weekday']
    time_df = time_data
    time_df.columns = column_labels

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df.columns = ['user_id', 'firstname', 'lastname', 'gender', 'level']

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        songplay_data = ( row.ts, row.userId, row.level, song_id, artist_id, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: This function lists all the json files in the filepath which is provided as an argument. It then iterates over all of the files in the directories and then processes all the
    data from the files and inserts it into the tables in a specified database.

    Arguments:
        cur: the cursor object.
        conn: connection to the database.
        filepath: log data / song data file directories.
        func: function that transforms the data and inserts it into the database.

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
        """
        Description: This is the starting point of the program. This connects to the sparkify database and creates a cursor used for executing our SQL queries.
        This then runs the data processing job and finally closes the connection to our database.
        
        """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()