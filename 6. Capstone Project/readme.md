# Udacity Data Engineer Nanodegree - Capstone Project

## Step 1: Scope the Project and Gather Data

### Scope

This project will use Spark and Postgres in order to provide a clean datawarehouse for analytics purpose.
The main goal of this datawarehouse is to provide data about immigration data.

### Datasets

| Data Set | Format | Description |
| ---      | ---    | ---         |
|[I94 Immigration Data](https://travel.trade.gov/research/reports/i94/historical/2016.html)| SAS | Data contains international visitor arrival statistics by world regions and select countries (including top 20), type of visa, mode of transportation, age groups, states visited (first intended address only), and the top ports of entry (for select countries).|
|[I94_SAS_Labels_Descriptions](https://openflights.org/data.html)| Raw text | This dataset contains a description of the I94 Immigration data|
|[U.S. City Demographic Data](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)| CSV | This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000.|
|[Airports](https://datahub.io/core/airport-codes#data)| CSV | This dataset contains information about airports|
|[Airlines](https://openflights.org/data.html#airline)| CSV | This dataset contains information about airlines|

## Step 2: Explore and Assess the Data

## Step 3: Define the Data Model

- immigration table
      root
      |-- cod_port: string (nullable = true)
      |-- cod_state: string (nullable = true)
      |-- dtadfile: string (nullable = true)
      |-- visapost: string (nullable = true)
      |-- occup: string (nullable = true)
      |-- entdepa: string (nullable = true)
      |-- entdepd: string (nullable = true)
      |-- entdepu: string (nullable = true)
      |-- matflag: string (nullable = true)
      |-- dtaddto: string (nullable = true)
      |-- gender: string (nullable = true)
      |-- insnum: string (nullable = true)
      |-- airline: string (nullable = true)
      |-- admnum: double (nullable = true)
      |-- fltno: string (nullable = true)
      |-- visatype: string (nullable = true)
      |-- immigration_id: long (nullable = false)
      |-- cic_id: integer (nullable = true)
      |-- cod_visa: integer (nullable = true)
      |-- cod_mode: integer (nullable = true)
      |-- cod_country_origin: integer (nullable = true)
      |-- cod_country_cit: integer (nullable = true)
      |-- year: integer (nullable = true)
      |-- month: integer (nullable = true)
      |-- bird_year: integer (nullable = true)
      |-- age: integer (nullable = true)
      |-- counter: integer (nullable = true)
      |-- arrival_date: date (nullable = true)
      |-- departure_date: date (nullable = true)

- Airlines table
      root
      |-- _c0: string (nullable = true)
      |-- _c1: string (nullable = true)
      |-- _c2: string (nullable = true)
      |-- _c3: string (nullable = true)
      |-- _c4: string (nullable = true)
      |-- _c5: string (nullable = true)
      |-- _c6: string (nullable = true)
      |-- _c7: string (nullable = true)
      |-- airline_id: string (nullable = true)
      |-- airline_name: string (nullable = true)
      |-- alias: string (nullable = true)
      |-- iata: string (nullable = true)
      |-- ica0: string (nullable = true)
      |-- callsign: string (nullable = true)
      |-- country: string (nullable = true)
      |-- active: string (nullable = true)

- Airports table
      root
      |-- ident: string (nullable = true)
      |-- type: string (nullable = true)
      |-- name: string (nullable = true)
      |-- elevation_ft: float (nullable = true)
      |-- continent: string (nullable = true)
      |-- iso_country: string (nullable = true)
      |-- iso_region: string (nullable = true)
      |-- municipality: string (nullable = true)
      |-- gps_code: string (nullable = true)
      |-- iata_code: string (nullable = true)
      |-- local_code: string (nullable = true)
      |-- coordinates: string (nullable = true)

- Country table
      root
      |-- code: string (nullable = true)
      |-- country: string (nullable = true)

- City table
      root
      |-- code: string (nullable = true)
      |-- city: string (nullable = true)

- State table
      root
      |-- code: string (nullable = true)
      |-- state: string (nullable = true)

## Step 4: Run ETL to Model the Data
## Step 5: Complete Project Write Up

---
