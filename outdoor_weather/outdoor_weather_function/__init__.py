import json
import requests
import threading
import datetime
from datetime import timedelta
import pendulum
import pandas as pd
import logging
import pyrebase
import azure.functions as func


def get_outdoor_weather_data(station_key="", start_time=pendulum.datetime(2021, 6, 1, tz='Asia/Bangkok'), end_time=pendulum.now('Asia/Bangkok')):
    dt = pendulum.now('Asia/Bangkok')
    _valid_time = start_time
    _end_time = end_time
    Data = {}

    def worker(dt):
        a = requests.get("https://api.weather.com/v1/location/{}:9:TH/observations/historical.json?apiKey=6532d6454b8aa370768e63d6ba5a832e&units=e&startDate={}".format(station_key, dt))
        b = json.loads(a.text)
        c = pd.DataFrame(b["observations"])
        Data[dt] = c
        
    num_th = 20
    while _end_time > _valid_time:
        
        threads = []        
        for i in range(num_th):            
            if _end_time <= _valid_time:
                break
            
            dt = _valid_time.strftime("%Y%m%d")
            t = threading.Thread(target=worker, args=(dt,))
            threads.append(t)       
            _valid_time = _valid_time.add(days=1)
            
        for t in threads:
            t.start()           
        for t in threads:
            t.join()

    df = pd.concat([Data[k] for k in Data], axis=0)

    # Remove columns with NULL more than 90% data as null.
    cols_to_delete = df.columns[df.isnull().sum()/len(df) > .90]
    df.drop(cols_to_delete, axis = 1, inplace = True)

    df = df.sort_values(by="valid_time_gmt")

    df["datetime"] = pd.to_datetime(df["valid_time_gmt"].astype(int)*1e9) + timedelta(hours=7)
    df = df.reset_index(drop=True)
    df = df.set_index("datetime")

    # Drop irrelevant columns
    columns = ['key', 'class', 'expire_time_gmt', 'obs_id', 'obs_name']
    df.drop(columns=columns, inplace=True)

    return df


def fahrenheit_to_celsius(temperature):
    celsius = (temperature-32)*5/9
    return round(celsius, 2)


def pressure_inHg_to_mbar(pressure):
    mbar = pressure * 33.8639
    return round(mbar, 2)


def mph_to_kmh(wind_speed):
    kmh = wind_speed * 1.6
    return round(kmh, 2)


def main(mytimer: func.TimerRequest) -> None:
    _now = pendulum.now(tz='Asia/Bangkok')

    # if mytimer.past_due:
    #     logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', _now)

    # Step 1 : connect to firebase
    firebaseConfig = {
        "apiKey": "AIzaSyAyM699YBXl2aZfO5eNmuK4H2UXst5QePI",
        "authDomain": "altolido-ec6c7.firebaseapp.com",
        "databaseURL": "https://altolido-ec6c7-default-rtdb.asia-southeast1.firebasedatabase.app",
        "projectId": "altolido-ec6c7",
        "storageBucket": "altolido-ec6c7.appspot.com",
        "messagingSenderId": "528262137451",
        "appId": "1:528262137451:web:0f9153e00889f1fcf5532e",
        "measurementId": "G-K4BTJ05H8Q"
    }
    fb = pyrebase.initialize_app(firebaseConfig)
    fb_db = fb.database()

    # Step 2 : collect outdoor weather data
    station_key = "VTBD"
    end_time = pendulum.now('Asia/Bangkok')
    start_time = end_time.subtract(hours=1)
    df_outdoor_weather = get_outdoor_weather_data(station_key, start_time, end_time)

    # Step 3 : create latest data to update to firebase
    last_row = df_outdoor_weather.iloc[-1]
    firebase_data = {
        "updated_at": int(last_row["valid_time_gmt"]),
        "location": "BANGKOK/VTBD",
        "air_quality_index": 70,
        "precipitation": float(fahrenheit_to_celsius(last_row["dewPt"])),
        "pressure": {
            "unit": "mbar",
            "value":  float(pressure_inHg_to_mbar(last_row["pressure"]))
        },
        "relative_humidity": {
            "unit": "%",
            "value": float(last_row["rh"])
        },
        "temperature": {
            "unit": "celcius",
            "value": float(fahrenheit_to_celsius(last_row["temp"]))
        },
        "wind_speed": {
            "unit": "km/h",
            "value": float(mph_to_kmh(last_row["wspd"]))
        },
        "weather": last_row['wx_phrase']
    }

    # Step 4 : upload outdoor weather data to firebase
    fb_db.child("building/lido/pages/dashboard/outdoor_weather").update(firebase_data)
    logging.info(f"updated latest outdoor weather data to firebase at {_now}")
