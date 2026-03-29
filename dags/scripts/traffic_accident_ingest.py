from dags.scripts.data_utils import connect_to_db, disconnect_from_db
from psycopg2.extras import execute_batch
import pandas as pd
import datetime
import json
import requests
import logging
import os
import math

logger = logging.getLogger(__name__)
 

def safe_json(record):
    def convert(value):
        if isinstance(value, float) and math.isnan(value):
            return None
        return value
    return {k: convert(v) for k, v in record.items()}


def download_csv(url, city):
    timestamp = int(datetime.datetime.now().timestamp())
    save_folder = r"C:\sql\traffic-accident-datawarehouse\data"
    save_path = os.path.join(save_folder, f"traffic_accident_{city}_{timestamp}.csv")
    try:
        os.makedirs(save_folder, exist_ok=True)
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(save_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return save_path
    except Exception as e:
        logger.error(f"Error while downloading file: {e}")
        

def traffic_accident_ingest():
    # download new dataset
    file_path = download_csv("https://data.brno.cz/api/download/v1/items/298c37feb1064873abdccdc2a10b605f/csv?layers=0", "brno")
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        # convert csv to json and save to db
        df = pd.read_csv(file_path, encoding="utf-8", chunksize=1000)
        for chunk in df:
            records = chunk.to_dict(orient="records")
            data = [
                (json.dumps(safe_json(record)), os.path.basename(file_path))
                for record in records
            ]
            execute_batch(cur, """
                INSERT INTO bronze.traffic_accidents_raw (payload, source)
                VALUES (%s::jsonb, %s)
            """, data)
        conn.commit()
    except Exception as e:
        logger.error(f"Error while inserting data: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

traffic_accident_ingest()
