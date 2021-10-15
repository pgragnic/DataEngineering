CREATE TABLE dim_countries (
    Country text PRIMARY KEY,
    Region text,
    Population integer,
    Area integer,
    PopulationDensity text,
    Coastline real,
    GDP real,
    Literacy bigint,
    Phones real,
    Arable real,
    Crops real,
    Other real,
    Climate text,
    Birthrate text,
    Deathrate text,
    Agriculture text,
    Industry text,
    Service text,
    InfantMortality real,
    NetMigration real
);

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

CREATE TABLE fact_covid (
    fact_covid_id PRIMARY KEY
    Province_State text,
    Country text,
    Last_Update date,
    Long_ text,
    Confirmed integer,
    Deaths integer,
    Recovered integer,
    Active integer,
    Combined_Key text,
    Incident_Rate real,
    Case_Fatality_Ratio real,
    Longtitude real,
    Latitude real
);