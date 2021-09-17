import configparser
import psycopg2
import time
from sql_queries import copy_table_queries, insert_table_queries
from create_tables import create_tables, drop_tables


def load_staging_tables(cur, conn):

    """
    Description: This function loads json files from a S3 bucket into staging tables in a Redshift cluster 
    
    Arguments:
        cur: the cursor object
        conn: connection to the database

    Returns:
        None
    """

    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):

    """
    Description: This function inserts data from staging tables into 5 final tables
    
    Arguments:
        cur: the cursor object
        conn: connection to the database

    Returns:
        None
    """

    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    '''
    print("1. Dropping tables...")
    start_time = time.time()
    drop_tables(cur, conn)
    print("--- It took %s seconds ---" % (time.time() - start_time))

    print("2. Creating tables...")
    start_time = time.time()
    create_tables(cur, conn)
    print("--- It took %s seconds ---" % (time.time() - start_time))

    print("3. Loading staging tables, it may take time...")
    start_time = time.time()
    load_staging_tables(cur, conn)
    print("--- It took %s seconds ---" % (time.time() - start_time))
    '''
    print("4. Loading final tables...")
    start_time = time.time()
    insert_tables(cur, conn)
    print("--- It took %s seconds ---" % (time.time() - start_time))

    conn.close()

if __name__ == "__main__":
    main()