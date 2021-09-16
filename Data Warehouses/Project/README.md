# Summary

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Datasets
This project is based on 2 datasets:
- Song Dataset: subset of real data from the Million Song Dataset. 
- Log Dataset: activity logs from a music streaming app

# Prerequisites

- python 3.x
- python modules:
  - configparser
  - psycopg2
  - pandas

# Files used

- create_tables.py: create sparkify database and create tables
- etl.ipynb: used to prepare the effective etl process
- etl.py: contains the effective etl process
- readme.md: contains overview and instructions about this etl process
- sql_queries: contains all drop, create, insert and find SQL queries used by etl process
- test.ipynb: script which tests if data have been correctly inserted by etl process 
- data folder:
  - log_data: contain activity data
  - song_data: contain data about songs (artist, duration...)

# Database schema

Staging tables:
  - staging_events
  - staging_songs

The star schema has:
- 1 fact table: 
  - songplays
- 4 dimension tables
  - users
  - songs
  - artists
  - time

![](sparkify_erd.png?raw=true)

# How To
- create a project folder
- install "psycopg2-binary" python library: pip install psycopg2-binary
- install "pandas" python library: pip install pandas
- unzip the file in the project folder
- start a terminal in the project folder
- run command `python .\create_redshift_cluster.py`
- run command `python .\etl.py` (it will call automatically create_tables.py first)
When Redshift cluster is not anymore necessary
- run command `python .\delete_redshift_cluster.py` 
