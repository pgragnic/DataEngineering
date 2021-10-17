

**Type**

**Nullable**

No

**Field Name**

fact\_covid\_id

country

Integer

Text

Yes

Yes

province\_state

last\_update

confirmed

deaths

Text

Date

Yes

Integer

Integer

Yes

Yes

recovered

Integer

Integer

Yes

active

Yes

Yes

Yes

Yes

Yes

Text

combined\_key

case\_fatality\_ratio

longitude

Float

Float

Float

latitude





**Comment**

Unique Key

Country Name

province or state if applicable

YYYY/MM/DD

Counts include confirmed and probable (where reported).

Recovered cases are estimates based on local media reports, and state and

local reporting when available, and therefore may be substantially lower than

the true number

Active cases = total cases - total recovered - total deaths. This value is for

reference only after we stopped to report the recovered cases (see [Issue

#4465](https://github.com/CSSEGISandData/COVID-19/issues/4465))

country/province when applicable

Case-Fatality Ratio (%) = Number recorded deaths / Number cases.

Longitude

Latitude





**Field Name**

country

**Type**

Text

Text

**Nullable**

No

region

Yes

population

area

Integer Yes

Integer Yes

populationdensity

gdp

Text

Yes

Float Yes

climate

Text

Text

Text

Text

Text

Text

Yes

Yes

Yes

Yes

Yes

Yes

birthrate

deathrate

agriculture

industry

service





**Comment**

Country name

Region name

Population total

Size in square kilometers

Population density

GDP measures the total market value (gross) of all domestic goods and services produced (product) in a given year

Climate type

Birth rate ratio

Death rate ratio

Agriculture ratio

Industry ratio

Service ratio





ar.





**Field Name**

**Type**

**Nullable**

No

country

Text

ghrp

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Yes

Text

income\_classification

Text

net\_oda\_received\_perc\_of\_gni

aid\_dependence

Float

Float

Float

Float

Float

Float

Float

Float

Float

Float

Float

Float

Float

Float

Float

Float

Float

Float

remittances

food\_imports\_percent\_of\_total\_merchandise\_exports

food\_import\_dependence

fuels\_ores\_and\_metals\_exports

primary\_commodity\_export\_dependence

tourism\_as\_percentage\_of\_gdp

tourism\_dependence

general\_government\_gross\_debt\_percent\_of\_gdp\_2019

government\_indeptedness

total\_reserves\_in\_months\_of\_imports\_2018

foreign\_currency\_reserves

foreign\_direct\_investment\_net\_inflows\_percent\_of\_gdp

foreign\_direct\_investment

covid\_19\_economic\_exposure\_index

covid\_19\_economic\_exposure\_index\_ex\_aid\_and\_fdi

covid\_19\_economic\_exposure\_index\_ex\_aid\_and\_fdi\_an

d\_food\_import

Yes

Yes

volume\_of\_remittances

Float





**Comment**

Country

GHRP

Income classification according to WB

Net ODA received (% of GNI)

Aid dependence

Remittances

Food imports (% of total merchandise exports)

food import dependence

Fuels, ores and metals exports (% of total merchandise exports))

primary commodity export dependence

tourism as percentage of GDP

Tourism dependence

General government gross debt (Percent of GDP) - 2019

Government indeptedness

Total reserves in months of imports - 2018

Foreign currency reserves

Foreign direct investment, net inflows (% of GDP)

Foreign direct investment

Covid 19 Economic exposure index

Covid 19 Economic exposure index [ex aid and FDI]

Covid 19 Economic exposure index [ex aid and FDI and food import]

Volume of remittances (in USD) as a proportion of total GDP (%) 2014-18





**Field Name**

**Type**

Text

Text

Date

**Nullable**

No

location

Yes

iso\_code

Yes

date

total\_vaccinations

Integer

Float

Yes

Yes

total\_vaccinations\_per\_hundred

daily\_vaccinations\_raw

Inetger

Yes

daily\_vaccinations

Integer

Integer

Yes

Yes

daily\_vaccinations\_per\_million

people\_vaccinated

Integer

Float

Yes

Yes

people\_vaccinated\_per\_hundred

people\_fully\_vaccinated

Integer

Integer

Yes

Yes

people\_fully\_vaccinated\_per\_hundred

total\_boosters

Integer

Float

Yes

Yes

total\_boosters\_per\_hundred





**Comment**

name of the country (or region within a country).

ISO 3166-1 alpha-3 – three-letter country codes.

date of the observation.

total number of doses administered. For vaccines that require multiple doses, each individual dose is counted. If

a person receives one dose of the vaccine, this metric goes up by 1. If they receive a second dose, it goes up by 1

total\_vaccinations per 100 people in the total population of the country.

daily change in the total number of doses administered. It is only calculated for consecutive days. This is a raw

measure provided for data checks and transparency, but we strongly recommend that any analysis on daily

new doses administered per day (7-day smoothed). For countries that don't report data on a daily basis, we

assume that doses changed equally on a daily basis over any periods in which no data was reported. This

produces a complete series of daily figures, which is then averaged over a rolling 7-day window. An example of

daily\_vaccinations per 1,000,000 people in the total population of the country.

total number of people who received at least one vaccine dose. If a person receives the first dose of a 2-dose

vaccine, this metric goes up by 1. If they receive the second dose, the metric stays the same.

people\_vaccinated per 100 people in the total population of the country.

total number of people who received all doses prescribed by the vaccination protocol. If a person receives the

first dose of a 2-dose vaccine, this metric stays the same. If they receive the second dose, the metric goes up by

people\_fully\_vaccinated per 100 people in the total population of the country.

Total number of COVID-19 vaccination booster doses administered (doses administered beyond the number

prescribed by the vaccination protocol)

Total number of COVID-19 vaccination booster doses administered per 100 people in the total population.

