from data_utils import connect_to_db, disconnect_from_db
import datetime
import json
import logging

logger = logging.getLogger(__name__)
 
def traffic_accident(backfill=False):
    if backfill:
        start_year = 2010
        end_year = datetime.datetime.now().year - 1
        for year in range(start_year, end_year + 1):
            traffic_accident_ingest(f'nehody_{year}01-{year}12')
    else:
        year = datetime.datetime.now().year - 1
        traffic_accident_ingest(f'nehody_{year}01-{year}12')

def traffic_accident_ingest(data_file):
    conn, cur = None, None
    try:
        # Load file
        file_path = fr'.\data\accidents\{data_file}.geojson'
        with open(file_path, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        # insert data into database
        conn, cur = connect_to_db()
        cur.execute("""
            INSERT INTO bronze.traffic_accidents_raw (payload, source)
            VALUES (%s::jsonb, %s)
        """, (json.dumps(geojson_data), data_file))
        conn.commit()
        logger.info(f"Data from {file_path} inserted successfully.")
    except Exception as e:
        logger.error(f"Error while inserting data: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

# traffic_accident(backfill=True)
