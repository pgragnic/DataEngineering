# Summary

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Datasets
This project is based on 2 datasets:
- Song Dataset: subset of real data from the Million Song Dataset. 
- Log Dataset: activity logs from a music streaming app

# Prerequisites

- AWS EMR cluster or local Spark
- python 3.x
- python modules:
  - configparser

# Files used

- dl.cfg: contains AWS credentials
- etl.py: reads data from S3, processes that data using Spark, and writes them back to S3
- readme.md: contains overview and instructions about this etl process

# Parquet files schema

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
- install "configparser" python library: pip install configparser
- install "pandas" python library: pip install pandas
- unzip the file in the project folder
- start a terminal in the project folder
- run command `python .\etl.py`