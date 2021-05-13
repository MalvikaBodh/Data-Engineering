import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This function will load our staging tables and we're using copy command to load staging data from s3"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """This function will use our newly created staging tables and those will be used to insert data into our new star schema tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This function will connect to our cluster using configurations in dwh.cfg and then run both the load table and insert table functions"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()