from data_utils import connect_to_db, disconnect_from_db
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def dim_city():
    conn, cur = None, None
    cities = ["Brno"]
    try:
        conn, cur = connect_to_db()
        # Insert Ciities
        for city in cities:
            cur.execute(f"""
                INSERT INTO gold.dim_city (
                        city_name
                ) VALUES (%s)
                ON CONFLICT (city_name) DO NOTHING;
            """, (city,))
            conn.commit()
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

def dim_time():
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        start = datetime(2010, 1, 1)
        end = datetime(2030, 12, 31)
        # Insert data
        cur.execute("""
            INSERT INTO gold.dim_time (
                    date,
                    year,
                    month,
                    day,
                    day_of_week,
                    hour
            )
            SELECT
                d::date AS date,
                EXTRACT(YEAR FROM d) AS year,
                EXTRACT(MONTH FROM d) AS month,
                EXTRACT(DAY FROM d) AS day,
                EXTRACT(DOW FROM d) AS day_of_week,
                h.hour
            FROM generate_series(
                %s::timestamp,
                %s::timestamp,
                interval '1 day'
            ) AS d
            CROSS JOIN generate_series(0, 23) AS h(hour)
            ON CONFLICT DO NOTHING;
            """, (start, end))
        conn.commit()
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

# dim_city()
dim_time()