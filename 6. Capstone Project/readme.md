# Capstone project about COVID

- [Capstone project about COVID](#capstone-project-about-covid)
  - [Step 1: Scope the Project and Gather Data](#step-1-scope-the-project-and-gather-data)
    - [Scope](#scope)
    - [Data](#data)
      - [JHU CSSE COVID-19 Dataset](#jhu-csse-covid-19-dataset)
        - [Covid-19 Economic Exposure Index](#covid-19-economic-exposure-index)
        - [Countries of the World](#countries-of-the-world)
        - [Coronavirus (COVID-19) Vaccinations](#coronavirus-covid-19-vaccinations)
  - [Step 2: Explore and Assess the Data](#step-2-explore-and-assess-the-data)
    - [Cleanup tasks](#cleanup-tasks)
  - [Step 3: Define the Data Model](#step-3-define-the-data-model)
    - [Data model](#data-model)
    - [Data Pipeline](#data-pipeline)
  - [Step4: Run ETL to Model the Data](#step4-run-etl-to-model-the-data)

## Step 1: Scope the Project and Gather Data

### Scope

Johns Hopkins University (JHU) is a reference for COVID19 data.
The goal of this project is to provide a Datawarehouse containing information coming from JHU and aggregate with other sources, vaccination, economic exposure and countries.

### Data

Data is composed of 4 datasets.

#### [JHU CSSE COVID-19 Dataset](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data)

This is the data repository for the 2019 Novel Coronavirus Visual Dashboard operated by the Johns Hopkins University Center for Systems Science and Engineering (JHU CSSE). Also, Supported by ESRI Living Atlas Team and the Johns Hopkins University Applied Physics Lab (JHU APL).

This folder contains daily case reports. All timestamps are in UTC (GMT+0).

##### [Covid-19 Economic Exposure Index](https://data.humdata.org/dataset/covid-19-economic-exposure-index)

Country's economic exposure due to COVID-19. Composite indicator based on World Bank's datasets on remittances, food import dependence, primary commodity export dependence, tourism dependence, government indebtedness and foreign currency reserves.

##### [Countries of the World](https://www.kaggle.com/fernandol/countries-of-the-world)

Information on population, region, area size, infant mortality and more.

##### [Coronavirus (COVID-19) Vaccinations](https://ourworldindata.org/covid-vaccinations)

This vaccination dataset uses the most recent official numbers from governments and health ministries worldwide. Population estimates for per-capita metrics are based on the United Nations World Population Prospects. Income groups are based on the World Bank classification. A full list of our country-specific sources is available at the bottom of this page, and we also answer frequently-asked questions there.

In this dataset you can see all of the data on COVID-19 vaccinations (doses administered, people with at least 1 dose, and people fully vaccinated).

## Step 2: Explore and Assess the Data

### Cleanup tasks

Covid19 dataset

- Remove FIPS and Admin2 of JHU CSSE COVID-19 Dataset as it is US only information
- Convert date values to date
- Convert Last_Update to datetime
- Rename Long_ field to Longitude and Lat to Latitude
- Convert Confirmed, Deaths, Recovered and Active to integer
- Convert Longitude, Latitude, Incident_Rate and Case_Fatality_Ratio to float

Countries dataset

- Rename columns with spaces
- Convert fields to integer or numeric depending of their type

Exposure dataset

- Replace spaces by _ in column names
- Replace , by . for numbers in Spark dataframe
- Replace "x", "No data" and "0" by NULL
- Convert fields to integer or float depending of their type

Vaccination

- Rename location column to country
- Convert fields to integer or numeric depending of their type

## Step 3: Define the Data Model

- Map out the conceptual data model and explain why you chose that model
- List the steps necessary to pipeline the data into the chosen data model

### Data model

Since the purpose of this data warehouse is for OLAP and BI app usage, we will model these data sets with star schema data modeling.

See ERD below:
![ERD](images/erd.png)

### Data Pipeline

![Data Pipeline](images/data-pipeline.png)

- Reads all the CSV files from "data\csse_covid_19_data\csse_covid_19_daily_reports\" to fill fact_covid table
- Read static CSV files:
  - countries.csv in dim_countries
  - exposure.csv in dim_exposure
  - vaccination.csv on dim_vaccination

## Step4: Run ETL to Model the Data
