import os
import requests
import json
import datetime
from datetime import timedelta
import time
from data_utils import connect_to_db, disconnect_from_db
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

def get_station_id(file):
    with open(file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    unique_stations = sorted(list({city['station_id'] for city in data}))
    return unique_stations

def weather_ingest(backfill=False):
    station_ids = get_station_id("./data/cities.json")
    conn, cur = connect_to_db()
    
    try:
        for station_id in station_ids:
            print(f"--- Processing station {station_id} ---")
            
            if backfill:
                start_date = datetime.date(2010, 1, 1)
                end_date = datetime.date.today() - timedelta(days=1)
                
                current = start_date
                while current <= end_date:
                    batch_end = min(current + timedelta(days=30), end_date)
                    
                    print(f"  Fetching {current} to {batch_end}...")
                    fetch_and_store_weather_hourly(cur, station_id, current, batch_end)
                    
                    conn.commit()
                    current = batch_end + timedelta(days=1)
            else:
                yesterday = datetime.date.today() - timedelta(days=1)
                fetch_and_store_weather_hourly(cur, station_id, yesterday, yesterday)
                conn.commit()
                
    finally:
        disconnect_from_db(conn, cur)

def fetch_and_store_weather_hourly(cur, station_id, start_date, end_date):
    API_KEY = os.getenv("METEOSTAT_API_KEY")
    url = "https://meteostat.p.rapidapi.com/stations/hourly"
    
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "meteostat.p.rapidapi.com"
    }
    
    params = {
        "station": station_id,
        "start": start_date.strftime("%Y-%m-%d"),
        "end": end_date.strftime("%Y-%m-%d"),
        "tz": "Europe/Prague"
    }

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        raw_data = response.json()
        records = raw_data.get("data", [])

        if records:
            db_data = [
                (json.dumps(record), f"meteostat_hourly_{station_id}")
                for record in records
            ]

            execute_batch(cur, """
                INSERT INTO bronze.meteostat_raw (payload, source)
                VALUES (%s::jsonb, %s)
            """, db_data)
            print(f"  -> Saved {len(records)} hours for station {station_id}")
        else:
            print(f"  -- No hourly data found for {station_id} in period {start_date}")

        time.sleep(0.2)

    except Exception as e:
        print(f"  !! Error for {station_id}: {e}")

if __name__ == "__main__":
    weather_ingest(backfill=False)