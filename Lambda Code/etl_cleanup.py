# this is the second function for the Morrisville dataset. Here the raw data is extracted from the RDS and cleaned up before loading into a new database.

import json
import os
import psycopg2
import pandas as pd
import numpy as np
from psycopg2.extras import Json


ENDPOINT1 = os.environ['ENDPOINT1']
DB_NAME1 = os.environ['DB_NAME1']
ENDPOINT2 = os.environ['ENDPOINT2']
DB_NAME2 = os.environ['DB_NAME2']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']



def lambda_handler(event, context):
    # Connect to the postgres database with raw data
    try:
        conn1 = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT1, DB_NAME1, USERNAME, PASSWORD))

    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur1 = conn1.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
        
    
    # Connect to the new postgres database
    try:
        conn2 = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT2, DB_NAME2, USERNAME, PASSWORD))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
        
    try:
        cur2 = conn2.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
        

    # Auto commit
    conn1.set_session(autocommit=True)
    conn2.set_session(autocommit=True)
    
    
    #Get data from database with raw data
    try:
        cur1.execute("SELECT * FROM morrisville;")
    except psycopg2.Error as e:
        print(f"Error executing the query. Error: {e}")
        
    #load the data into a dataframe for the cleanup
    columns = [desc[0] for desc in cur1.description]
    data_raw = cur1.fetchall()
    df1 = pd.DataFrame(data_raw, columns=columns)
    
    #close connection to the first database
    cur1.close()
    conn1.close()
    
    #Transform/cleanup
    #remove columns that are not relevant for the final data structure
    columns_to_drop = ['neighborhood', 'state', 'reported', 'id', 'tract', 'zone', 'zip', 'asst_officers']
    df1.drop(columns=columns_to_drop, inplace=True)
    
    #Create new table and load data into new database   
    try:

        
        # Delete "morrisville" table
        cur2.execute("DROP TABLE IF EXISTS morrisville;")
        
        #Create new table
        cur2.execute("CREATE TABLE IF NOT EXISTS morrisville (incident_id SERIAL PRIMARY KEY, occurred timestamp, weekday text, month text, year int, offense text, street text, city text, subdivision text, district text, latitude float, longitude float);")
        
        conn2.commit()
    
        # Insert the data into the PostgreSQL table
        for index, row in df1.iterrows():
            occurred_value = row['occurred']
            if pd.isna(occurred_value):
                occurred_value = None  # Set to None for proper handling by psycopg2

            query = "INSERT INTO morrisville (incident_id, occurred, weekday, month, year, offense, street, city, subdivision, district, latitude, longitude) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            values = (
                occurred_value,
                row['weekday'],
                row['month'],
                row['year'],
                row['offense'],
                row['street'],
                row['city'],
                row['subdivision'],
                row['district'],
                row['area']['lat'],
                row['area']['lon']
            )
            try:
                cur2.execute(query, values)
            except psycopg2.Error as e:
                print(f"An error occurred: {str(e)}")
    
    except psycopg2.Error as e:
        print(f"An error occurred: {str(e)}")

    # add a new column with timestamp rounded to full hours so that we can later on join it with the waeather dataset which only has hourly data
    cur2.execute("ALTER TABLE morrisville ADD COLUMN rounded_timestamp TIMESTAMP;")
    cur2.execute("UPDATE morrisville SET rounded_timestamp = CASE WHEN EXTRACT(minute FROM occurred) < 30 THEN date_trunc('hour', occurred) ELSE date_trunc('hour', occurred) + interval '1 hour' END;")
    
    # Update values in the "city" column
    cur2.execute("UPDATE morrisville SET city = 'MORRISVILLE' WHERE city IN('MORR', 'MORRISVILLE N DURHAM', 'MORRISILLE', 'MORRIVILLE');")
    cur2.execute("DELETE FROM morrisville WHERE city IS NULL or city IN ('RALEIGH', '<Redacted>', 'CLAYTON', 'DURHAM');")
    
    # Remove entries with missing timestamp
    cur2.execute("DELETE FROM morrisville WHERE rounded_timestamp IS NULL;")
    
    # Commit the changes to the database
    conn2.commit()

    # Close the cursor and the database connection
    cur2.close()
    conn2.close()

