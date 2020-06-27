import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function  is responsible for loading data
    from AWS S3 into the staging tables:
    stage_events and stage_songs
    inputs:
           cur: pyscopg2 cursor object for query execution
           conn: a connection object return by psycopg2 
    output:
         Returns None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function  is responsible for extracting data from the staged_tables
    stage_events and stage_songs into the fact and dimension tables.
    inputs:
           cur: pyscopg2 cursor object for query execution
           conn: a connection object return by psycopg2 
    output:
         Returns None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function  is responsible for 
    extablishing connection to the database and starting the 
    ETL processes.
    It closes the connection the database once done.
   
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()