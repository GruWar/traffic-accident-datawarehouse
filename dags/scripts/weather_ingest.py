from data_utils import connect_to_db, disconnect_from_db
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os
import requests
import logging
import json
import datetime
from datetime import timedelta
import time

load_dotenv()
logger = logging.getLogger(__name__)

def fetch_and_store_weather(station_id, start_date, end_date):

    # --- CONFIG ---
    API_KEY = os.getenv("METEOSTAT_API_KEY")

    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": "meteostat.p.rapidapi.com"
    }
    params = {
        "station": station_id,
        "start": start_date.strftime("%Y-%m-%d"),
        "end": end_date.strftime("%Y-%m-%d"),
        "tz": "Europe/Prague",
        "units": "metric"
    }
    url = "https://meteostat.p.rapidapi.com/stations/hourly"
    conn, cur = None, None
    try:
        # Get data from meteostat API
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        payload_json = response.json()

        records = payload_json.get("data", [])
        if not records:
            logger.warning(f"No data for {start_date} - {end_date}")
            return
        data = [
            (json.dumps(record), f"meteostat_{station_id}_{start_date}_{end_date}")
            for record in records
        ]

        # Save to db
        conn, cur = connect_to_db()
        execute_batch(cur, """
            INSERT INTO bronze.meteostat_raw (payload, source)
            VALUES (%s::jsonb, %s)
        """, data)
        conn.commit()
        time.sleep(1)
    except Exception as e:
        logger.error(f"Error while fetching data: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

def meteostat_backfill(start_date, end_date, station_id):
    current = start_date

    while current < end_date:
        batch_end = current + datetime.timedelta(days=30)

        fetch_and_store_weather(
            station_id,
            current,
            min(batch_end, end_date)
        )
        current = batch_end

def meteostat_incremental(station_id):
    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    fetch_and_store_weather(
        station_id,
        yesterday,
        yesterday
    )

# test
end_date = datetime.date.today() - timedelta(days=1)
start_date = datetime.date(2010, 1, 1)

# meteostat_backfill(start_date,end_date,11723)