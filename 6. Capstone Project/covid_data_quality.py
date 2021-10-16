import psycopg2
import configparser
from sql_queries import Data_quality_queries


def data_quality_check(dim_countries_limit, dim_exposure_limit, dim_vaccination_limit, fact_covid_limit):

    for query in Data_quality_queries:
        cur.execute(query)
        records = cur.fetchone()
        table = query.replace("SELECT COUNT (*) FROM ","")
        table = table.replace(";","")

        # Check that tables returned more than 0 rows
        if records[0] < 1:
            print("1")
            raise ValueError(f"Data quality check failed. query {table} returned 0 rows")
        else:
            print(f"Data quality for table \"{table}\" check passed with {records[0]} records")
        
        # Check row number depending on the table expected size (may vary in the future)

        if table == "dim_countries" and records[0] < dim_countries_limit:
            print(f"Number of row {records[0]} is under {dim_countries_limit} for table \"{table}\"")

        if table == "dim_exposure" and records[0] < dim_exposure_limit:
            print(f"Number of row {records[0]} is under {dim_exposure_limit} for table \"{table}\"")
        
        if table == "dim_vaccination" and records[0] < dim_vaccination_limit:
            print(f"Number of row {records[0]} is under {dim_vaccination_limit} for table \"{table}\"")

        if table == "fact_covid" and records[0] < fact_covid_limit:
            print(f"Number of row {records[0]} is under {fact_covid_limit} for table \"{table}\"")


def main():
    dim_countries_limit = 200
    dim_exposure_limit = 180
    dim_vaccination_limit = 50000
    fact_covid_limit = 2000000
    
    config = configparser.ConfigParser()
    config.read("parameters.cfg")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['postgresql_sql'].values()))
    cur = conn.cursor()

    data_quality_check(dim_countries_limit, dim_exposure_limit, dim_vaccination_limit, fact_covid_limit)

if __name__ == "__main__":
    main()
