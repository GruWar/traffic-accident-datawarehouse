from data_utils import connect_to_db, disconnect_from_db
import logging

logger = logging.getLogger(__name__)

def weather_data_clean(table_name):
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        cur.execute(f"""
             INSERT INTO silver.weather_clean (
                station_id,
                date,
                time,
                temp_c,
                precipitation_mm,
                snow,
                wind_dir,
                wind_speed
            )
            SELECT
                substring(source FROM '(D?\\d+)$') AS station_id,
                (payload::json ->> 'time')::timestamp::date AS date,
                (payload::json ->> 'time')::timestamp::time AS time,
                (payload::json ->> 'temp')::numeric AS temp_c,
                (payload::json ->> 'prcp')::numeric AS precipitation_mm,
                (payload::json ->> 'snow')::numeric AS snow,
                (payload::json ->> 'wdir')::numeric AS wind_dir,
                (payload::json ->> 'wspd')::numeric AS wind_speed
            FROM bronze.{table_name}
            ON CONFLICT (station_id, date, time) DO NOTHING;
        """)
        conn.commit()
        logger.info(f"Successfully inserted data into silver.weather_clean")
    except Exception as e:
        logger.error(f"Error Inserting into database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

weather_data_clean("meteostat_raw")