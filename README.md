# Morrisville Crime Data Warehouse

Real-Time ETL, Data Warehousing & Visualization on AWS

## Project Overview

This project was developed as part of a course on Data Lake and Data Warehouse, where a complete real-time data warehouse pipeline for crime data in Morrisville, NC was built. The project ingests crime data from an open public API, processes it through an ETL pipeline using AWS Lambda, stores raw and cleaned data in PostgreSQL databases hosted on Amazon RDS, and integrates it into a data warehouse. The final data is visualized in Tableau.

## Data Source

Morrisville Crime Dataset (Live)

    API: https://opendata.townofmorrisville.org/api/explore/v2.1/catalog/datasets/pd_incident_report/exports/json
    Fields Used: date_occu, offense, location, latitude, longitude, etc.
    Additional: Weather data integrated from a separate historical dataset for context.

## Components

🧩 **ETL Pipeline (Lambda Functions)**

    * etl_pipeline.py: Extracts raw crime data from Morrisville’s OpenData API and stores it in a PostgreSQL raw data lake (via RDS).
    * etl_cleanup.py: Cleans the raw data by dropping irrelevant fields, standardizing values (e.g., city names), handling missing timestamps, and outputs
    a cleaned dataset into a new PostgreSQL instance.
    * load_dwh.py: Merges Morrisville and Cary crime datasets and loads them into a consolidated table tbl_crimes in preparation for DWH modeling.
    * dwh_create.py: Builds a star schema in a data warehouse by creating fact and dimension tables (for crime, location, date), and populates them from
    the cleaned data.
    * weather_dwh.py: Enriches the fact table with hourly weather data by adding a new dim_weather dimension and connecting it via timestamp.

## Cloud Architecture (AWS)

    **Lambda:** Serverless compute to run each stage of the ETL pipeline.
    **RDS PostgreSQL:** Hosted raw and cleaned crime data + final data warehouse.
    **Environment Variables:** Secured DB credentials and endpoints are managed via AWS Lambda configuration.

## Data Warehouse Design (Star Schema)
Table |	Type | Description
dim_location | Dimension | Geolocation attributes (city, district, lat/lon)
dim_date | Dimension | Timestamp attributes (weekday, month, year)
dim_crime | Dimension | Crime categories (e.g., Assault, Theft)
dim_weather | Dimension | Temperature, rain, cloud cover, wind, etc.
factless_fact | Fact (main) | Joins all IDs and links weather to crime
