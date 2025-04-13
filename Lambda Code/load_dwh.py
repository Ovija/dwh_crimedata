# in this function both crime datasets are merged together so that it is later on easier to transfer into the datawarehouse.

import json
import os
import psycopg2
import pandas as pd
import numpy as np
from psycopg2.extras import Json


MORR_ENDPOINT = os.environ['MORR_ENDPOINT']
MORR_DB = os.environ['MORR_DB']
OV_USERNAME = os.environ['OV_USERNAME']
OV_PASSWORD = os.environ['OV_PASSWORD']
CAR_ENDPOINT = os.environ['CAR_ENDPOINT']
CAR_DB = os.environ['CAR_DB']
AS_USERNAME = os.environ['AS_USERNAME']
AS_PASSWORD = os.environ['AS_PASSWORD']

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
        
        
    # Connect to the cary database
    try:
        car_conn = psycopg2.connect("host={} dbname={} user={} password={}".format(CAR_ENDPOINT, CAR_DB, AS_USERNAME, AS_PASSWORD))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
        
    try:
        car_cur = car_conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
    

    # Auto commit
    mor_conn.set_session(autocommit=True)
    car_conn.set_session(autocommit=True)
    
    
    #Get data from morrisville database
    
    try:
        mor_cur.execute("SELECT * FROM morrisville WHERE occurred >= '2021-01-01';")
    except psycopg2.Error as e:
        print(f"Error executing the query. Error: {e}")
        
    #load the data into a dataframe for the cleanup
    columns1 = [desc[0] for desc in mor_cur.description]
    data_raw1 = mor_cur.fetchall()
    df_mor = pd.DataFrame(data_raw1, columns=columns1)

    #Get data from cary database
    try:
        car_cur.execute("SELECT * FROM clean_data_gold_2;")
    except psycopg2.Error as e:
        print(f"Error executing the query. Error: {e}")
        
    #load the data into a dataframe for the cleanup
    columns2 = [desc[0] for desc in car_cur.description]
    data_raw2 = car_cur.fetchall()
    df_car = pd.DataFrame(data_raw2, columns=columns2)
        

    #create new table merging both incident datasets and adding id to it
    #first get data from morrisville and add to new table
    try:
        # Delete "morrisville" table
        mor_cur.execute("DROP TABLE IF EXISTS tbl_crimes;")
        mor_conn.commit()
        
        mor_cur.execute("CREATE TABLE IF NOT EXISTS tbl_crimes (incident_id SERIAL PRIMARY KEY, datetime timestamp, rounded_time timestamp, weekday text, crime_type text, street text, city text, subdivision text, district text, latitude float, longitude float);")
        mor_conn.commit()
    
        # Insert the data into the PostgreSQL table
        for index, row in df_mor.iterrows():
            occurred_value = row['occurred']
            rounded_value = row['rounded_timestamp']
            if pd.isna(occurred_value):
                occurred_value = None  # Set to None for proper handling by psycopg2
            if pd.isna(rounded_value):
                rounded_value = None  # Set to None for proper handling by psycopg2
            query = "INSERT INTO tbl_crimes (incident_id, datetime, rounded_time, weekday, crime_type, street, city, subdivision, district, latitude, longitude) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            values = (
                occurred_value,
                rounded_value,
                row['weekday'],
                row['offense'],
                row['street'],
                row['city'],
                row['subdivision'],
                row['district'],
                row['longitude'],
                row['latitude']
            )
            try:
                mor_cur.execute(query, values)
            except psycopg2.Error as e:
                print(f"An error occurred: {str(e)}")
    
    except psycopg2.Error as e:
        print(f"An error occurred: {str(e)}")
    
    #get data from cary and add to new table
    try:
        # Insert the data into the PostgreSQL table
        for index, row in df_car.iterrows():
            date_str = row['date_from'].strftime('%Y-%m-%d')
            time_str = row['from_time'].strftime('%H:%M:%S')
            rounded_str = row['from_time_rounded'].strftime('%H:%M:%S')
            datetime_value = date_str + ' ' + time_str
            rounded_time_value = date_str + ' ' + rounded_str
            query = "INSERT INTO tbl_crimes (incident_id, datetime, rounded_time, weekday, crime_type, street, city, subdivision, district, latitude, longitude) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            values = (
                datetime_value,
                rounded_time_value,
                row['crimeday'],
                row['crime_type'],
                row['geocode'],
                'CARY',
                row['subdivisn_id'],
                row['district'],
                row['lat'],
                row['lon']
            )
            try:
                mor_cur.execute(query, values)
            except psycopg2.Error as e:
                print(f"An error occurred: {str(e)}")
    
    except psycopg2.Error as e:
        print(f"An error occurred: {str(e)}")
        
    #delete when date is missing
    delete_query = "DELETE FROM tbl_crimes WHERE rounded_time IS NULL;"
    mor_cur.execute(delete_query)
    mor_conn.commit()
    

    #close connection to the first database
    mor_cur.close()
    mor_conn.close()
    
    car_cur.close()
    car_conn.close()
