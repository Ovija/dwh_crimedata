# create datawarehouse and load the crime data into the datawarehouse

import json
import os
import psycopg2
import pandas as pd
import numpy as np
from psycopg2.extras import Json


MORR_ENDPOINT = os.environ['MORR_ENDPOINT']
MORR_DB = os.environ['MORR_DB']
DWH_ENDPOINT = os.environ['DWH_ENDPOINT']
DWH_NAME = os.environ['DWH_NAME']
OV_USERNAME = os.environ['OV_USERNAME']
OV_PASSWORD = os.environ['OV_PASSWORD']


def lambda_handler(event, context):
    # Connect to the morrisville database
    try:
        mor_conn = psycopg2.connect("host={} dbname={} user={} password={}".format(MORR_ENDPOINT, MORR_DB, OV_USERNAME, OV_PASSWORD))

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        mor_cur = mor_conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    
    # Connect to the dwh
    try:
        dwh_conn = psycopg2.connect("host={} dbname={} user={} password={}".format(DWH_ENDPOINT, DWH_NAME, OV_USERNAME, OV_PASSWORD))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
        
    try:
        dwh_cur = dwh_conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
        

    # Auto commit
    mor_conn.set_session(autocommit=True)
    dwh_conn.set_session(autocommit=True)
    
    # create the fact and dimension tables
    
    dwh_cur.execute("CREATE TABLE IF NOT EXISTS dim_location (location_id SERIAL PRIMARY KEY, city text, street text, subdivision text, district text, latitude float, longitude float);")
    dwh_cur.execute("CREATE TABLE IF NOT EXISTS dim_crime (crime_id INT PRIMARY KEY, crime_type text);")
    dwh_cur.execute("CREATE TABLE IF NOT EXISTS dim_date (rounded_time timestamp PRIMARY KEY, weekday text, month text, year int);")
    dwh_cur.execute("CREATE TABLE IF NOT EXISTS factless_fact (fact_id SERIAL PRIMARY KEY, crime_fk INT REFERENCES dim_crime(crime_id), date_fk timestamp REFERENCES dim_date(rounded_time), location_fk INT REFERENCES dim_location(location_id));")
    
    dwh_conn.commit()
    
    
    #Get data from morrisville database
    try:
        mor_cur.execute("SELECT * FROM tbl_crimes;")
    except psycopg2.Error as e:
        print(f"Error executing the query. Error: {e}")
        
    #load the data into a dataframe
    columns = [desc[0] for desc in mor_cur.description]
    data_raw = mor_cur.fetchall()
    df_mor = pd.DataFrame(data_raw, columns=columns)
    
    #check if the data already exists in the different dimension tables, if yes get the ID
    check_query_loc = "SELECT location_id FROM dim_location WHERE latitude = %s AND longitude = %s;"
    check_query_dat = "SELECT rounded_time FROM dim_date WHERE rounded_time = %s;"
    check_query_inc = "SELECT crime_id FROM dim_crime WHERE crime_type = %s;"

    fact_records = []

    #load the data into the dwh
    for index, row in df_mor.iterrows():
        #this is the value used for the check of the existing entries defined above
        check_values_loc = (row['latitude'], row['longitude'])
        check_values_dat = (row['rounded_time'],)
        check_values_inc = (row['crime_type'],)
        

        # execute the check and get the existing IDs (if possible)
        dwh_cur.execute(check_query_loc, check_values_loc)
        existing_location_id = dwh_cur.fetchone()
        
        dwh_cur.execute(check_query_dat, check_values_dat)
        existing_date_id = dwh_cur.fetchone()
        
        dwh_cur.execute(check_query_inc, check_values_inc)
        existing_crime_id = dwh_cur.fetchone()

        # if the data doesn't in the location dimension, make a new entry
        if existing_location_id is None:
            query_loc = "INSERT INTO dim_location (location_id, city, street, subdivision, district, latitude, longitude) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s);"
            values_loc = (
                row['city'],
                row['street'],
                row['subdivision'],
                row['district'],
                row['latitude'],
                row['longitude']
                )
            try:
                dwh_cur.execute(query_loc, values_loc)
                dwh_conn.commit()
            except psycopg2.Error as e:
                print(f"An error occurred: {str(e)}")
        
        #now make again a select for the ID
        dwh_cur.execute(check_query_loc, check_values_loc)
        #save the id for the later use
        location_id = dwh_cur.fetchone()
        
        # if the data doesn't in the date dimension, make a new entry
        if existing_date_id is None:
            year_value = row['rounded_time'].year
            month_value = row['rounded_time'].month
            query_date = "INSERT INTO dim_date (rounded_time, weekday, month, year) VALUES (%s, %s, %s, %s);"
            values_date = (
                row['rounded_time'],
                row['weekday'],
                month_value,
                year_value
                )
            try:
                dwh_cur.execute(query_date, values_date)
                dwh_conn.commit()
            except psycopg2.Error as e:
                print(f"An error occurred: {str(e)}")
                
        #now make again a select for the ID
        dwh_cur.execute(check_query_dat, check_values_dat)
        #save the id for the later use
        date_id = dwh_cur.fetchone()
        
        # if the data doesn't in the crime dimension, make a new entry
        if existing_crime_id is None:
            query_incident = "INSERT INTO dim_crime (crime_id, crime_type) VALUES (%s, %s);"
            values_incident = (
                row['incident_id'],
                row['crime_type']
                )
            try:
                dwh_cur.execute(query_incident, values_incident)
                dwh_conn.commit()
            except psycopg2.Error as e:
                print(f"An error occurred: {str(e)}")
                
        #now make again a select for the ID    
        dwh_cur.execute(check_query_inc, check_values_inc)
        #save the id for the later use
        crime_id = dwh_cur.fetchone()
        
        # Append fact record (all three IDs from the different dimension tables) to the list
        fact_records.append({
            'crime_fk': crime_id,
            'date_fk': date_id,
            'location_fk': location_id
        })
    
    fact_df = pd.DataFrame(fact_records)
    
    try:
        #delete existing records from the fact table
        dwh_cur.execute("DELETE FROM factless_fact;")
        dwh_conn.commit()
        
        #load the IDs into the facttable and create a new ID as PK
        for _, fact_row in fact_df.iterrows():
            query_fact = "INSERT INTO factless_fact (fact_id, crime_fk, date_fk, location_fk) VALUES (DEFAULT, %s, %s, %s);"
            values_fact = (
                fact_row['crime_fk'], 
                fact_row['date_fk'],
                fact_row['location_fk']
                )
            dwh_cur.execute(query_fact, values_fact)
    
        dwh_conn.commit()
    except Exception as e:
        print(f"Error writing fact table to PostgreSQL: {e}")


    #close connection to the  database
    mor_cur.close()
    mor_conn.close()
    
    dwh_cur.close()
    dwh_conn.close()















    