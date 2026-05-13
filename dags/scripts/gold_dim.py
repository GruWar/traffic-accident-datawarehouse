from data_utils import connect_to_db, disconnect_from_db
import logging
import json
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

def city_dim_load():
    # Load json file
    with open('data/cities.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Connect to DB
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()

        # Insert data into DB
        data_to_insert = [(item['city'], item['region'], item['lat'], item['lon'], item['station_id']) for item in data]
        insert_query = """
            INSERT INTO gold.dim_city (city_name, region, lat, lon, station_id)
            VALUES %s
        """
        execute_values(cur, insert_query, data_to_insert)
        conn.commit()
    except Exception as e:
        logger.error(f"Error while inserting data: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

if __name__ == "__main__":
    city_dim_load()