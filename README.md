# Week 1 – Data Engineering Internship Assignment

## Overview
This project implements a basic ETL pipeline that extracts CSV and API data, performs cleaning and transformation, and loads processed data into Google Cloud Storage (GCS).

## Datasets
- clickstream.csv – Website activity (~200k rows)
- transactions.csv – Purchase records (~100k rows)
- ExchangeRate API – Real-time currency conversion

## Pipeline Steps

### Extract
- Read CSV files in chunks using pandas
- Fetch real-time currency data from API

### Transform
- Standardize column names
- Remove duplicates
- Convert transaction amounts to USD
- Add logging for record tracking

### Load
- Save cleaned data partitioned by ingest_date
- Upload to Google Cloud Storage bucket

## Architecture

![Architecture](architecture.png)

## Tools Used
- Python
- Pandas
- Google Cloud Storage
- VS Code
- Git & GitHub

## Results
- Successfully processed datasets
- Uploaded partitioned outputs to GCS
- Logs included for record counts and warnings
