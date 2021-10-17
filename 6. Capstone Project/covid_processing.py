from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date, col, trim, ltrim, rtrim, when, regexp_replace, monotonically_increasing_id
from pyspark.sql.utils import AnalysisException

import os
import configparser
import pandas as pd
import psycopg2
import time

from create_tables import create_tables, drop_tables


def create_spark_session():

    """
    Description: This function  creates or get (if already exists) a Spark session 

    Arguments:
        None

    Returns:
        spark: Spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.4") \
        .getOrCreate()
    return spark


def create_tables_func(cur, conn):

    """
    Description: This function scraps COVID csv files from Github

    Arguments:
        cur: cursor
        conn: connection to the DB
    
    Returns:
        None
    """

    print("Dropping tables...")
    drop_tables(cur, conn)
 
    print("Creating tables...")
    create_tables(cur, conn)


def get_covid_data(covid_data, spark, table, db_url, db_user, db_password):

    """
    Description: This function scraps COVID csv files from Github

    Arguments:
        covid_data: type of files to process
        spark: spark session
        table: target table

    Returns:
        None
    """

    # read covid data and write it in postgres
    print ("Reading covid data file") 
    df = spark.read.csv(covid_data, header=True, inferSchema=True)
    print(df.columns)

    print("Writing covid data to Postgres")
    df.select("*") \
        .withColumn("fact_covid_id", monotonically_increasing_id()) \
        .withColumn("country", col("country_region")) \
        .withColumn("Longtitude", col("Long_").cast("float")) \
        .withColumn("Latitude", col("Lat").cast("float")) \
        .withColumn("Case_Fatality_Ratio", col("Case_Fatality_Ratio").cast("float")) \
        .withColumn("Confirmed", col("Confirmed").cast("integer")) \
        .withColumn("Deaths", col("Deaths").cast("integer")) \
        .withColumn("Recovered", col("Recovered").cast("integer")) \
        .withColumn("Active", col("Active").cast("integer")) \
        .withColumn("Last_Update", col("Last_Update").cast("date")) \
        .drop("FIPS", "Admin2", "Long_", "Lat", "country_region","incident_rate", "incidence_rate") \
        .write \
        .mode("append") \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", "fact_covid") \
        .option("user", db_user) \
        .option("password", db_password) \
        .save()


def get_countries(file_path, spark, table, db_url, db_user, db_password):

    """
    Description: This function imports countries data

    Arguments:
        covid_data: type of files to process
        spark: spark session
        table: target table
        db_url: url of the Postgres database
        db_user: db username
        db_pasword: db password

    Returns:
        None
    """

    print("Creating countries table")

    df_countries = spark \
        .read \
        .format("csv", ) \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .load(file_path)

    df_countries.select("*") \
        .withColumn("Population", col("Population").cast("integer")) \
        .withColumn("Area", col("Area").cast("integer")) \
        .withColumn("Country", rtrim(col("Country"))) \
        .withColumn("Region", rtrim(col("Region"))) \
        .withColumn("GDP", col("GDP").cast("float")) \
        .withColumn("Literacy", col("Literacy").cast("long")) \
        .withColumn("Phones", col("Phones").cast("float")) \
        .withColumn("Arable", col("Arable").cast("float")) \
        .withColumn("Crops", col("Crops").cast("float")) \
        .withColumn("Other", col("Other").cast("float")) \
        .drop("Net migration", "Infant mortality", "Coastline",\
            "literacy", "phones","arable","crops","other") \
        .write \
        .mode("append") \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .save()

def get_exposure(file_path, spark, table, db_url, db_user, db_password):

    """
    Description: This function imports economic exposure data

    Arguments:
        covid_data: type of files to process
        spark: spark session
        table: target table
        db_url: url of the Postgres database
        db_user: db username
        db_pasword: db password

    Returns:
        None
    """
    
    print("Creating exposure table")

    pd_df_exposure = pd.read_csv(file_path, sep=";", encoding="ISO-8859-1", dtype=str)
    pd_df_exposure = pd_df_exposure.replace({'x': None, 'No data': None, '0': None})
    pd_df_exposure = pd_df_exposure.apply(lambda x: x.str.replace(',','.'))
    pd_df_exposure.columns = pd_df_exposure.columns.str.strip()
    pd_df_exposure.columns = pd_df_exposure.columns.str.replace("[ ]", "_")
    pd_df_exposure.columns = pd_df_exposure.columns.str.replace("[,]", "")

    df_exposure = spark.createDataFrame(pd_df_exposure.astype(str))

    df_exposure.select("*") \
        .withColumn("Net_ODA_received_perc_of_GNI", col("Net_ODA_received_perc_of_GNI").cast("float")) \
        .withColumn("Aid_dependence", col("Aid_dependence").cast("float")) \
        .withColumn("Volume_of_remittances", col("Volume_of_remittances_in_USD_as_a_proportion_of_total_GDP_percent_2014-18").cast("float")) \
        .withColumn("Remittances", col("Remittances").cast("float")) \
        .withColumn("Food_imports_percent_of_total_merchandise_exports", col("Food_imports_percent_of_total_merchandise_exports").cast("float")) \
        .withColumn("food_import_dependence", col("food_import_dependence").cast("float")) \
        .withColumn("Fuels_ores_and_metals_exports", col("Fuels_ores_and_metals_exports").cast("float")) \
        .withColumn("primary_commodity_export_dependence", col("primary_commodity_export_dependence").cast("float")) \
        .withColumn("tourism_as_percentage_of_GDP", col("tourism_as_percentage_of_GDP").cast("float")) \
        .withColumn("tourism_dependence", col("tourism_dependence").cast("float")) \
        .withColumn("General_government_gross_debt_Percent_of_GDP_2019", col("General_government_gross_debt_Percent_of_GDP_2019").cast("float")) \
        .withColumn("Government_indeptedness", col("Government_indeptedness").cast("float")) \
        .withColumn("Total_reserves_in_months_of_imports_2018", col("Total_reserves_in_months_of_imports_2018").cast("float")) \
        .withColumn("Foreign_currency_reserves", col("Foreign_currency_reserves").cast("float")) \
        .withColumn("Foreign_direct_investment_net_inflows_percent_of_GDP", col("Foreign_direct_investment_net_inflows_percent_of_GDP").cast("float")) \
        .withColumn("Foreign_direct_investment", col("Foreign_direct_investment").cast("float")) \
        .withColumn("Covid_19_Economic_exposure_index", col("Covid_19_Economic_exposure_index").cast("float")) \
        .withColumn("Covid_19_Economic_exposure_index", col("Covid_19_Economic_exposure_index").cast("float")) \
        .withColumn("Covid_19_Economic_exposure_index_Ex_aid_and_FDI", col("Covid_19_Economic_exposure_index_Ex_aid_and_FDI").cast("float")) \
        .withColumn("Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import", col("Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import").cast("float")) \
        .drop("Volume_of_remittances_in_USD_as_a_proportion_of_total_GDP_percent_2014-18") \
        .write \
        .mode("append") \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .save()


def get_vaccination(file_path, spark, table, db_url, db_user, db_password):

    print("Creating vaccination table")

    df_vaccination = spark \
        .read \
        .format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .load(file_path)

    df_vaccination.select("*") \
        .withColumn("country", col("location")) \
        .withColumn("date", regexp_replace("date", "/", "").cast("date")) \
        .withColumn("total_vaccinations", col("total_vaccinations").cast("integer")) \
        .withColumn("people_vaccinated", col("people_vaccinated").cast("integer")) \
        .withColumn("people_fully_vaccinated", col("people_fully_vaccinated").cast("integer")) \
        .withColumn("total_boosters", col("total_boosters").cast("integer")) \
        .withColumn("daily_vaccinations_raw", col("daily_vaccinations_raw").cast("integer")) \
        .withColumn("daily_vaccinations", col("daily_vaccinations").cast("integer")) \
        .withColumn("total_vaccinations_per_hundred", col("total_vaccinations_per_hundred").cast("float")) \
        .withColumn("people_vaccinated_per_hundred", col("people_vaccinated_per_hundred").cast("float")) \
        .withColumn("people_fully_vaccinated_per_hundred", col("people_fully_vaccinated_per_hundred").cast("float")) \
        .withColumn("total_boosters_per_hundred", col("total_boosters_per_hundred").cast("float")) \
        .withColumn("daily_vaccinations_per_million", col("daily_vaccinations_per_million").cast("integer")) \
        .drop("location") \
        .write \
        .mode("append") \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .save()


def main():
    # Get Postgres parameters
    db_properties={}
    config = configparser.ConfigParser()
    config.read("parameters.cfg")
    db_prop = config['postgresql_jdbc']
    db_url = db_prop['url']
    db_user = db_prop['username']
    db_password = db_prop['password']
    db_properties['username']=db_prop['username']
    db_properties['password']=db_prop['password']
    db_properties['url']=db_prop['url']

    # Connection to Postgres DB
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['postgresql_sql'].values()))
    cur = conn.cursor()

    # filepath to data
    data = "data/"
    input_data = "csse_covid_19_data/csse_covid_19_daily_reports/"
    covid_data = os.path.join(data + input_data, "*.csv")

    # Create tables
    create_tables_func(cur, conn)

    #Create Spark session
    spark = create_spark_session()

    # Import data to Posgres DB
    get_covid_data(covid_data, spark, "fact_covid", db_url, db_user, db_password)
    get_countries(data + "countries.csv", spark, "dim_countries", db_url, db_user, db_password)
    get_exposure(data + "exposure.csv", spark, "dim_exposure", db_url, db_user, db_password)
    get_vaccination(data + "vaccination.csv", spark, "dim_vaccination", db_url, db_user, db_password)

if __name__ == "__main__":
    main()