from scripts.data_utils import connect_to_db, disconnect_from_db
import logging
import json
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from airflow.decorators import task

logger = logging.getLogger(__name__)

@task
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
            ON CONFLICT (city_name) DO NOTHING;
        """
        execute_values(cur, insert_query, data_to_insert)
        conn.commit()
    except Exception as e:
        logger.error(f"Error while inserting data: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

@task
def generate_date_dim():
    start_date = datetime(2000, 1, 1)
    end_date = datetime(2030, 12, 31)

    data_to_insert = []
    current_date = start_date

    while current_date <= end_date:
        data_to_insert.append((
            current_date,
            current_date.year,
            current_date.month,
            current_date.day,
            current_date.isoweekday()
        ))
        current_date += timedelta(days=1)

    conn, cur = connect_to_db()
    try:
        cur.execute("TRUNCATE TABLE gold.dim_date RESTART IDENTITY;")
        
        insert_query = """
            INSERT INTO gold.dim_date (date, year, month, day, day_of_week)
            VALUES %s
            ON CONFLICT (date) DO NOTHING;
        """
        execute_values(cur, insert_query, data_to_insert)
        conn.commit()
        print(f"Vygenerováno {len(data_to_insert)} dní.")
    except Exception as e:
        conn.rollback()
        print(f"Chyba: {e}")
    finally:
        disconnect_from_db(conn, cur)

@task
def road_dim_load(table_name):
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        cur.execute(f"""
        INSERT INTO gold.dim_road (
            road_id,
            road_type,
            is_intersection,
            lanes,
            max_speed,
            oneway
        )
        SELECT
            road_id,
            road_type,
            is_intersection,
            lanes,
            max_speed,
            oneway
            FROM silver.{table_name}
        ON CONFLICT (road_id) DO NOTHING;
        """)
        conn.commit()
        logger.info(f"Successfully inserted data into gold.dim_road")
    except Exception as e:
        logger.error(f"Error Inserting into database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

@task
def weather_dim_load(table_name):
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        cur.execute(f"""
        INSERT INTO gold.dim_weather (
            station_id,
            date,
            temp_c,
            precipitation_mm,
            snow,
            wind_dir,
            wind_speed,
            weather_category
        )
        SELECT
            station_id,
            date,
            trunc(avg(temp_c), 2) AS temp_c,
            trunc(avg(precipitation_mm), 2) AS precipitation_mm,
            trunc(avg(snow), 2) AS snow,
            avg(wind_dir)::int AS wind_dir,
            trunc(avg(wind_speed), 2) AS wind_speed,
            CASE
                WHEN avg(snow) > 1 THEN 'snowy'
                WHEN avg(precipitation_mm) > 10 AND avg(temp_c) > 0 THEN 'Heavy Rain'
                WHEN avg(precipitation_mm) BETWEEN 0.2 AND 10 AND avg(temp_c) > 0 THEN 'Rainy'
                WHEN avg(wind_speed) > 30 THEN 'Windy'
                WHEN avg(temp_c) < 0 AND avg(precipitation_mm) <= 0.2 THEN 'Freezing'
                ELSE 'Clear'
            END AS weather_category
        FROM silver.{table_name}
        GROUP BY station_id, date
        ON CONFLICT (station_id, date) DO NOTHING;;
        """)
        conn.commit()
        logger.info(f"Successfully inserted data into gold.dim_weather")
    except Exception as e:
        logger.error(f"Error Inserting into database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)