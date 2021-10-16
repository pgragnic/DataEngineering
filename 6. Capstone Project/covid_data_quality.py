import psycopg2
import configparser
from sql_queries import Data_quality_queries


def data_quality_check_zero(cur, conn):

    """
    Description: This function scraps COVID csv files from Github

    Arguments:
        cur: cursor
        conn: connection to the DB
    
    Returns:
        None
    """

    for query in Data_quality_queries:
        cur.execute(query)
        records = cur.fetchone()
        table = query.replace("SELECT COUNT (*) FROM ","")
        table = table.replace(";","")

        # Check that tables returned more than 0 rows
        if records[0] < 1:
            print(f"Data quality check failed. query {table} returned 0 rows")
        else:
            print(f"Data quality for table \"{table}\" check passed with {records[0]} records")

def data_quality_check_limit(cur, conn, dim_countries_limit, dim_exposure_limit, dim_vaccination_limit, fact_covid_limit):
    
    """
    Description: This function scraps COVID csv files from Github

    Arguments:
        cur: cursor
        conn: connection to the DB
        dim_countries_limit: lowest limit size permitted for dim_countries 
        dim_exposure_limit: lowest limit size permitted for dim_exposure
        dim_vaccination_limit: lowest limit size permitted for dim_exposure
        fact_covid_limit: lowest limit size permitted for fact_covid
    
    Returns:
        None
    """

    for query in Data_quality_queries:
        cur.execute(query)
        records = cur.fetchone()
        table = query.replace("SELECT COUNT (*) FROM ","")
        table = table.replace(";","")

        # Check row number depending on the table expected size (may vary in the future)

        if table == "dim_countries" and records[0] < dim_countries_limit:
            print(f"Number of row {records[0]} is under {dim_countries_limit} for table \"{table}\"")
        else:
            print(f"Number of row {records[0]} in table \"{table}\" is correct")

        if table == "dim_exposure" and records[0] < dim_exposure_limit:
            print(f"Number of row {records[0]} is under {dim_exposure_limit} for table \"{table}\"")
        else:
            print(f"Number of row {records[0]} in table \"{table}\" is correct")
        
        if table == "dim_vaccination" and records[0] < dim_vaccination_limit:
            print(f"Number of row {records[0]} is under {dim_vaccination_limit} for table \"{table}\"")
        else:
            print(f"Number of row {records[0]} in table \"{table}\" is correct")
        
        if table == "fact_covid" and records[0] < fact_covid_limit:
            print(f"Number of row {records[0]} is under {fact_covid_limit} for table \"{table}\"")
        else:
            print(f"Number of row {records[0]} in table \"{table}\" is correct")

def main():
    dim_countries_limit = 200
    dim_exposure_limit = 180
    dim_vaccination_limit = 50000
    fact_covid_limit = 2000000
    
    config = configparser.ConfigParser()
    config.read("parameters.cfg")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['postgresql_sql'].values()))
    cur = conn.cursor()

    data_quality_check_zero(cur, conn)
    data_quality_check_limit(cur, conn, dim_countries_limit, dim_exposure_limit, dim_vaccination_limit, fact_covid_limit)

if __name__ == "__main__":
    main()
