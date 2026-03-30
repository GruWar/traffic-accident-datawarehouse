from data_utils import connect_to_db, disconnect_from_db
import datetime
import json
import requests
import logging
import os

logger = logging.getLogger(__name__)
 

def download_json(url, city):
    data = None
    timestamp = int(datetime.datetime.now().timestamp())
    save_folder = r"C:\sql\traffic-accident-datawarehouse\data"
    save_path = os.path.join(save_folder, f"osm_{city}_{timestamp}.geojson")
    query = """
    [out:json];
    area["name"="Brno"]->.searchArea;
    (
    way["highway"](area.searchArea);
    );
    out body;
    >;
    out skel qt;    
    """
    try:
        response = requests.post(url, data=query)
        response.raise_for_status()

        os.makedirs(save_folder, exist_ok=True)

        data = response.json()
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
        if data:
            return save_path, data
    except Exception as e:
        logger.error(f"Error while downloading file: {e}")
        

def osm_ingest():
    # download new dataset
    file_path, data = download_json("https://overpass-api.de/api/interpreter", "brno")
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()

        cur.execute("""
            INSERT INTO bronze.osm_raw (payload, source)
            VALUES (%s, %s)
        """, (json.dumps(data), os.path.basename(file_path)))
        conn.commit()
    except Exception as e:
        logger.error(f"Error while inserting data: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

osm_ingest()
