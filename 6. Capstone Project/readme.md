# Capstone project about COVID

## Datasets

### [Daily reports (csse_covid_19_daily_reports)](https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports)

This folder contains daily case reports. All timestamps are in UTC (GMT+0).

#### File naming convention

MM-DD-YYYY.csv in UTC.

#### Field description

* FIPS: US only. Federal Information Processing Standards code that uniquely identifies counties within the USA.
* Admin2: County name. US only.
* Province_State: Province, state or dependency name.
* Country_Region: Country, region or sovereignty name. The names of locations included on the Website correspond with the official designations used by the U.S. Department of State.
* Last Update: MM/DD/YYYY HH:mm:ss  (24 hour format, in UTC).
* Lat and Long_: Dot locations on the dashboard. All points (except for Australia) shown on the map are based on geographic centroids, and are not representative of a specific address, building or any location at a spatial scale finer than a province/state. Australian dots are located at the centroid of the largest city in each state.
* Confirmed: Counts include confirmed and probable (where reported).
* Deaths: Counts include confirmed and probable (where reported).
* Recovered: Recovered cases are estimates based on local media reports, and state and local reporting when available, and therefore may be substantially lower than the true number. US state-level recovered cases are from [COVID Tracking Project](https://covidtracking.com/). We stopped to maintain the recovered cases (see [Issue #3464](https://github.com/CSSEGISandData/COVID-19/issues/3464) and [Issue #4465](https://github.com/CSSEGISandData/COVID-19/issues/4465)).
* Active: Active cases = total cases - total recovered - total deaths. This value is for reference only after we stopped to report the recovered cases (see [Issue #4465](https://github.com/CSSEGISandData/COVID-19/issues/4465))
* Incident_Rate: Incidence Rate = cases per 100,000 persons.
* Case_Fatality_Ratio (%): Case-Fatality Ratio (%) = Number recorded deaths / Number cases.
* All cases, deaths, and recoveries reported are based on the date of initial report. Exceptions to this are noted in the "Data Modification" and "Retrospective reporting of (probable) cases and deaths" subsections below.  

#### Update frequency

* Since June 15, We are moving the update time forward to occur between 04:45 and 05:15 GMT to accommodate daily updates from India's Ministry of Health and Family Welfare.
* Files on and after April 23, once per day between 03:30 and 04:00 UTC.
* Files from February 2 to April 22: once per day around 23:59 UTC.
* Files on and before February 1: the last updated files before 23:59 UTC. Sources: [archived_data](https://github.com/CSSEGISandData/COVID-19/tree/master/archived_data) and dashboard.

### [Covid-19 Economic Exposure Index](https://data.humdata.org/dataset/covid-19-economic-exposure-index)

Country's economic exposure due to COVID-19. Composite indicator based on World Bank's datasets on remittances, food import dependence, primary commodity export dependence, tourism dependence, government indebtedness and foreign currency reserves.

### [Countries of the World](https://www.kaggle.com/fernandol/countries-of-the-world)

Information on population, region, area size, infant mortality and more.

### [Coronavirus (COVID-19) Vaccinations](https://ourworldindata.org/covid-vaccinations)

This vaccination dataset uses the most recent official numbers from governments and health ministries worldwide. Population estimates for per-capita metrics are based on the United Nations World Population Prospects. Income groups are based on the World Bank classification. A full list of our country-specific sources is available at the bottom of this page, and we also answer frequently-asked questions there.

In this dataset you can see all of our data on COVID-19 vaccinations (doses administered, people with at least 1 dose, and people fully vaccinated).

* location: name of the country (or region within a country).
* iso_code: ISO 3166-1 alpha-3 – three-letter country codes.
* date: date of the observation.
* total_vaccinations: total number of doses administered. For vaccines that require multiple doses, each individual dose is counted. If a person receives one dose of the vaccine, this metric goes up by 1. If they receive a second dose, it goes up by 1 again. If they receive a third/booster dose, it goes up by 1 again.
* total_vaccinations_per_hundred: total_vaccinations per 100 people in the total population of the country.
* daily_vaccinations_raw: daily change in the total number of doses administered. It is only calculated for consecutive days. This is a raw measure provided for data checks and transparency, but we strongly recommend that any analysis on daily vaccination rates be conducted using daily_vaccinations instead.
* daily_vaccinations: new doses administered per day (7-day smoothed). For countries that don't report data on a daily basis, we assume that doses changed equally on a daily basis over any periods in which no data was reported. This produces a complete series of daily figures, which is then averaged over a rolling 7-day window. An example of how we perform this calculation can be found here.
* daily_vaccinations_per_million: daily_vaccinations per 1,000,000 people in the total population of the country.
* people_vaccinated: total number of people who received at least one vaccine dose. If a person receives the first dose of a 2-dose vaccine, this metric goes up by 1. If they receive the second dose, the metric stays the same.
* people_vaccinated_per_hundred: people_vaccinated per 100 people in the total population of the country.
* people_fully_vaccinated: total number of people who received all doses prescribed by the vaccination protocol. If a person receives the first dose of a 2-dose vaccine, this metric stays the same. If they receive the second dose, the metric goes up by 1.
* people_fully_vaccinated_per_hundred: people_fully_vaccinated per 100 people in the total population of the country.
* total_boosters: Total number of COVID-19 vaccination booster doses administered (doses administered beyond the number prescribed by the vaccination protocol)
* total_boosters_per_hundred: Total number of COVID-19 vaccination booster doses administered per 100 people in the total population.
