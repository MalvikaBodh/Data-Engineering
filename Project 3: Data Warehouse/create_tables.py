import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ This function will drop all the tables mentioned in drop_table_queries (sql_queries.py) list from our defined redshift cluster """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
     """ This function will create all the tables mentioned in create_table_queries (sql_queries.py) list from our defined redshift cluster """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    """This function will connect to our cluster using configurations in dwh.cfg and then run both the drop table and create table functions"""

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()