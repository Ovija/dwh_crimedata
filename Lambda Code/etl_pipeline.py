# this is the first lambda function for the dataset of Morrisville. Here the dataset is extracted via API from the source and loaded into a datalake (Postgres RDS) as raw data.

import json
import os
import psycopg2
import pandas as pd
import requests
import numpy
from psycopg2.extras import Json


ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']




def lambda_handler(event, context):
    # Connect to the postgres database
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
        

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
        

    # Auto commit
    conn.set_session(autocommit=True)
    
    
    # Get data from API
    # Define the API URL
    api_url = "https://opendata.townofmorrisville.org/api/explore/v2.1/catalog/datasets/pd_incident_report/exports/json"

    try:
        # Send a GET request to the API
        response = requests.get(api_url)
    
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Get the content (JSON data) from the response
            json_data = response.json()
        else:
            # If the request was not successful, print an error message
            print(f"Request failed with status code: {response.status_code}")
    except psycopg2.Error as e:
        print(f"An error occurred: {str(e)}")
        
    #Create table and load data into database   
    try:
        
        # Delete all records from the "morrisville" table
        cur.execute("DELETE FROM morrisville")
        
        #Create new table if it doesn't already exist
        cur.execute("CREATE TABLE IF NOT EXISTS morrisville (reported timestamp, occurred timestamp, weekday text, month text, year int, id int, offense text, street text, city text, state text, zip text, neighborhood text, subdivision text, tract text, zone text, district text, asst_officers text, area jsonb);")
        
        conn.commit()
    
        # Insert the data into the PostgreSQL table
        for entry in json_data:
            entry['area'] = json.dumps(entry['area'])
            cur.execute("INSERT INTO morrisville (reported, occurred, weekday, month, year, id, offense, street, city, state, zip, neighborhood, subdivision, tract, zone, district, asst_officers, area) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                entry['date_rept'],
                entry['date_occu'],
                entry['dow1'],
                entry['monthstamp'],
                entry['yearstamp'],
                entry['inci_id'],
                entry['offense'],
                entry['street'],
                entry['city'],
                entry['state'],
                entry['zip'],
                entry['neighborhd'],
                entry['subdivisn'],
                entry['tract'],
                entry['zone'],
                entry['district'],
                entry['asst_offcr'],
                entry['area']
            ))
    
    except psycopg2.Error as e:
        print(f"An error occurred: {str(e)}")

    # Commit the changes to the database
    conn.commit()

    # Close the cursor and the database connection
    cur.close()
    conn.close()
        
