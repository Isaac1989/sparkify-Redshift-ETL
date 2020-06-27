import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function  is responsible for dropping existing 
    tables
    inputs:
           cur: pyscopg2 cursor object for query execution
           conn: a connection object return by psycopg2 
    output:
         Returns None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function  is responsible for creating the required  
    tables. Eg. songplay, artist tables etc
    inputs:
           cur: pyscopg2 cursor object for query execution
           conn: a connection object return by psycopg2 
    output:
         Returns None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function  is responsible for 
    extablishing connection to the database and creating the required
    tables.
    It closes the connection to the database once done.
   
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()