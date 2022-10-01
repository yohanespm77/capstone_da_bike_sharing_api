from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd


app = Flask(__name__) 

@app.route('/homepage')
def home():
    return 'Hello World'

@app.route('/stations/')
def rute_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()    

@app.route('/trips/')
def rute__all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()  

@app.route('/json', methods=['POST']) 
def json_example():
    req = request.get_json(force=True)
    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result 

@app.route('/tripse/add', methods=['POST']) 
def route_add_trips():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/average_duration')
def rerata_trips():
    conn = make_connection()
    rata_rata = avg_trips(conn)
    return rata_rata.to_json()
    
@app.route('/trips/average_duration/<bike_id>') 
def rerata_bike_id(bike_id):
    conn = make_connection()
    rerata_bike_id = avg_bike_id(bike_id, conn)
    return rerata_bike_id.to_json()

@app.route('/tahun/<month>')
def tahun(month):
    conn = make_connection()
    agg = period(month, conn)
    return agg.to_json()

#------------------------------------------------------------------------------                  

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result
    
def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK' 

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'       

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result    

def avg_trips(conn):
    query = f"""
    SELECT trips.start_station_name AS Station_Name, AVG(duration_minutes) AS Rata_rata_durasi
    FROM trips
    GROUP BY Station_Name
    """
    result = pd.read_sql_query(query, conn, index_col='Station_Name')
    return result  

def avg_bike_id(bike_id, conn):
    query = f"""
    SELECT AVG(trips.duration_minutes) AS Avg_Bike_Id
    FROM trips 
    WHERE bikeid = {bike_id}"""
    result = pd.read_sql_query(query, conn)
    return result
    
def period(input_data, conn):
    tanggal = pd.read_sql_query(f"""
        SELECT *
        from trips
        WHERE Start_Time like '{input_data}%'
        """,conn)
    result = tanggal.groupby('subscriber_type').agg({
        'bikeid' : 'count', 
        'duration_minutes' : 'max'
    })
    result.rename(columns={'bikeid':'freq_of_bikeid','duration_minutes':'duration_max'},inplace=True)
    result['duration_max'] = result['duration_max'].astype('str') + ' minutes'
    return result  

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection    

if __name__ == '__main__':
    app.run(debug=True, port=5000)