from data_utils import connect_to_db, disconnect_from_db
from psycopg2.extras import execute_batch
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def get_db_data(table_name):
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        # Get data from db
        cur.execute(f"""
            SELECT raw_id, payload FROM bronze.{table_name};
        """)
        data = cur.fetchall()
        return data
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

def weather_data_clean():
    conn, cur = None, None
    data = []
    try:
        conn, cur = connect_to_db()
        # Get data
        payload = get_db_data("meteostat_raw")
        for record in payload:
            # Clean data
            dt = datetime.strptime(record["payload"]["time"], "%Y-%m-%d %H:%M:%S")
            date = dt.date()
            time = dt.time()
            row = [
                11723,
                date,
                time,
                record["payload"]["temp"],
                record["payload"]["prcp"],
                record["payload"]["snow"],
                record["payload"]["wdir"],
                record["payload"]["wspd"],
            ]
            data.append(row)

        # Insert data
        execute_batch(cur, """
            INSERT INTO silver.weather_clean (
                station_id,
                date,
                time,
                temp_c,
                precipitation_mm,
                snow,
                wind_dir,
                wind_speed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (station_id, date, time) DO NOTHING;
        """, data)
        conn.commit()

    except Exception as e:
        logger.error(f"Error while inserting: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)


weather_data_clean()