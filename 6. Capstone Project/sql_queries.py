# DROP TABLES
dim_countries_drop = "DROP TABLE IF EXISTS dim_countries"
dim_exposure_drop = "DROP TABLE IF EXISTS dim_exposure"
dim_vaccination_drop = "DROP TABLE IF EXISTS dim_vaccination"
fact_covid_drop = "DROP TABLE IF EXISTS fact_covid"

# CREATE TABLES

dim_countries_create = ("""
    CREATE TABLE dim_countries (
        Country text PRIMARY KEY,
        Region text,
        Population integer,
        Area integer,
        PopulationDensity text,
        GDP real,
        Climate text,
        Birthrate text,
        Deathrate text,
        Agriculture text,
        Industry text,
        Service text,
    );
""")

dim_exposure_create = ("""
    CREATE TABLE dim_exposure (
        country text PRIMARY KEY,
        GHRP text,
        Income_Classification text,
        Net_ODA_received_perc_of_GNI real,
        Aid_dependence real,
        Remittances real,
        Food_imports_percent_of_total_merchandise_exports real,
        food_import_dependence real,
        Fuels_ores_and_metals_exports real,
        primary_commodity_export_dependence real,
        tourism_as_percentage_of_GDP real,
        tourism_dependence real,
        General_government_gross_debt_Percent_of_GDP_2019 real,
        Government_indeptedness real,
        Total_reserves_in_months_of_imports_2018 real,
        Foreign_currency_reserves real,
        Foreign_direct_investment_net_inflows_percent_of_GDP real,
        Foreign_direct_investment real,
        Covid_19_Economic_exposure_index real,
        Covid_19_Economic_exposure_index_Ex_aid_and_FDI real,
        Covid_19_Economic_exposure_index_Ex_aid_and_FDI_and_food_import real,
        Volume_of_remittances real
    );
""")

dim_vaccination_create = ("""
    CREATE TABLE dim_vaccination (
        country text,
        iso_code text,
        date date,
        total_vaccinations integer,
        people_vaccinated integer,
        people_fully_vaccinated integer,
        total_boosters integer,
        daily_vaccinations_raw integer,
        daily_vaccinations integer,
        total_vaccinations_per_hundred real,
        people_vaccinated_per_hundred real,
        people_fully_vaccinated_per_hundred real,
        total_boosters_per_hundred real,
        daily_vaccinations_per_million integer,
        PRIMARY KEY(country, date)
    );
""")

fact_covid_create = ("""
    CREATE TABLE fact_covid (
        fact_covid_id bigint PRIMARY KEY,
        Country text,
        Province_State text,
        Last_Update date,
        Confirmed integer,
        Deaths integer,
        Recovered integer,
        Active integer,
        Combined_Key text,
        Case_Fatality_Ratio real,
        Longtitude real,
        Latitude real
    );
""")

# Data quality (DQ)

DQ_dim_countries = "SELECT COUNT(*) FROM dim_countries;"
DQ_dim_exposure = "SELECT COUNT(*) FROM dim_exposure;"
DQ_dim_vaccination = "SELECT COUNT(*) FROM dim_vaccination;"
DQ_fact_covid = "SELECT COUNT(*) FROM fact_covid;"

create_table_queries  = [dim_countries_create, dim_exposure_create, dim_vaccination_create, fact_covid_create]
drop_table_queries = [dim_countries_drop, dim_exposure_drop, dim_vaccination_drop, fact_covid_drop]
Data_quality_queries = [DQ_dim_countries, DQ_dim_exposure, DQ_dim_vaccination, DQ_fact_covid]
