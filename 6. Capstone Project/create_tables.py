import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):

    """
    Description: This function drops all tables from DB 
    
    Arguments:
        cur: the cursor object
        conn: connection to the database

    Returns:
        None
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    
    """
    Description: This function create all tables in DB 
    
    Arguments:
        cur: the cursor object
        conn: connection to the database

    Returns:
        None
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()