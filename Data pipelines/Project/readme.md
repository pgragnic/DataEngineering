# Data Pipeline with Airflow

## Summary

* [Introduction](#Introduction)
* [ELT Process](#ELT-Process)
* [Sources](#Sources)
* [Destinations](#Destinations)
* [Project Structure](#Project-Structure)
* [Data Quality Checks](#Data-Quality-Checks)

--------------------------------------------

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
They want to have this data available in a datawarehouse. The best solution to answer to this Business requirement is to host on a AWS Redshift Cluster.

## Datasets

This project is based on 2 datasets hosted on S3:

* Song Dataset: subset of real data from the Million Song Dataset.
* Log Dataset: activity logs from a music streaming app

## Prerequisites

* python 3.x
* python modules:
  * time
  * airflow

## ETL Process

The ETL process id managed by Airflow (<https://airflow.apache.org/>)

Schema of the Airflow DAG:
![DAG](images/dag.png?raw=true)

## Files used

### dag

* airflow_project.py: contains the DAG

### Operators

* data_quality.py: check data quality of all final tables
* load_dimension.py: load dimension tables (songs, users , artists, time)
* load_fact.py: load songplays table

### Helpers

* sql_queries.py: contains all create and select queries used by operators

## Database schema

Staging tables:

* staging_events
* staging_songs

The star schema has:

* 1 fact table:
  * songplays
* 4 dimension tables
  * users
  * songs
  * artists
  * time

![Sparkify ERD](images/sparkify_erd.png?raw=true)
