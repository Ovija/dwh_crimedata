# last function is adding the weather data to the dwh as a new dimension table

import json
import os
import psycopg2
import pandas as pd
import numpy as np
from psycopg2.extras import Json


DWH_ENDPOINT = os.environ['DWH_ENDPOINT']
DWH_NAME = os.environ['DWH_NAME']
OV_USERNAME = os.environ['OV_USERNAME']
WEA_ENDPOINT = os.environ['WEA_ENDPOINT']
WEA_DB = os.environ['WEA_DB']
PJ_USERNAME = os.environ['PJ_USERNAME']
PASSWORD = os.environ['PASSWORD']

def lambda_handler(event, context):
    #Connect to the weather database
    try:
        wea_conn = psycopg2.connect("host={} dbname={} user={} password={}".format(WEA_ENDPOINT, WEA_DB, PJ_USERNAME, PASSWORD))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
        
    try:
        wea_cur = wea_conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
       
    # Connect to the dwh
    try:
        dwh_conn = psycopg2.connect("host={} dbname={} user={} password={}".format(DWH_ENDPOINT, DWH_NAME, OV_USERNAME, PASSWORD))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)
        
    try:
        dwh_cur = dwh_conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)
        

    #Auto commit
    wea_conn.set_session(autocommit=True)
    dwh_conn.set_session(autocommit=True)
    
    #create the new dimension table inside the DWH and add the id column to the factless fact table
    dwh_cur.execute("CREATE TABLE IF NOT EXISTS dim_weather (weather_id SERIAL PRIMARY KEY, time timestamp, temp float, humidity int, dew_point float, apparent_temp float, precipitation float, rain float, snowfall float, snow_depth float, weather_code int, cloud_cover int, wind_speed float, is_day int);")
    dwh_conn.commit()

    dwh_cur.execute("ALTER TABLE factless_fact ADD COLUMN IF NOT EXISTS weather_fk int REFERENCES dim_weather(weather_id);")
    dwh_conn.commit()
    
    
    #Get the weather data from the datalake
    try:
        wea_cur.execute("SELECT * FROM weatherdataraw WHERE time >= '2021-01-01';")
    except psycopg2.Error as e:
        print(f"Error executing the query. Error: {e}")
    
    columns = [desc[0] for desc in wea_cur.description]
    wea_data = wea_cur.fetchall()
    df_wea = pd.DataFrame(wea_data, columns=columns)

    #check if the data already exists in the dimension table, if yes get the ID
    check_query_wea = "SELECT weather_id FROM dim_weather WHERE time = %s;"

    try:
        for index, row in df_wea.iterrows():
            #this is the value used for the check of the existing entries defined above
            check_values_wea = (row['time'],)
            
            # execute the check and get the existing IDs (if possible)
            dwh_cur.execute(check_query_wea, check_values_wea)
            existing_weather_id = dwh_cur.fetchone()
        
            if existing_weather_id is None:
                
                query = "INSERT INTO dim_weather (weather_id, time, temp, humidity, dew_point, apparent_temp, precipitation, rain, snowfall, snow_depth, weather_code, cloud_cover, wind_speed, is_day) VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                values = (
                    row['time'],
                    row['temperature_2m'],
                    row['relative_humidity_2m'],
                    row['dew_point_2m'],
                    row['apparent_temperature'],
                    row['precipitation'],
                    row['rain'],
                    row['snowfall'],
                    row['snow_depth'],
                    row['weather_code'],
                    row['cloud_cover'],
                    row['wind_speed_10m'],
                    row['is_day']
                )
                try:
                    dwh_cur.execute(query, values)
                except psycopg2.Error as e:
                    print(f"An error occurred: {str(e)}")
                    
                dwh_conn.commit()
                
        dwh_conn.commit()
    except Exception as e:
        print(f"Error writing fact table to PostgreSQL: {e}")
    
    update_query = "UPDATE factless_fact SET weather_fk = dim_weather.weather_id FROM dim_weather WHERE factless_fact.date_fk = dim_weather.time;"

    try:
        dwh_cur.execute(update_query)
    except psycopg2.Error as e:
        print(f"An error occurred: {str(e)}")

    # Commit the changes
    dwh_conn.commit()
    
    #close connection to the  database
    wea_cur.close()
    wea_conn.close()
    dwh_cur.close()
    dwh_conn.close()
    
