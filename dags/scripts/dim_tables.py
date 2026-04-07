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

def dim_road():
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        # Insert data
        cur.execute("""
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
            FROM silver.osm_roads_clean
            ON CONFLICT (road_id) DO NOTHING;
        """)

        conn.commit()
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

def dim_weather():
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        # Insert data
        cur.execute("""
            INSERT INTO gold.dim_weather (
                date,
                hour,
                temp_c,
                precipitation_mm,
                snow,
                wind_dir,
                wind_speed,
                weather_category
            )
            SELECT
                date,
                time AS hour,
                temp_c,
                precipitation_mm,
                snow,
                wind_dir,
                wind_speed,
                CASE
                    WHEN snow > 0 THEN 'SNOWY'
                    WHEN precipitation_mm > 0 THEN 'RAINY'
                    WHEN wind_speed > 10 THEN 'WINDY'
                    WHEN temp_c <= 0 THEN 'FREEZING'
                    ELSE 'CLEAR'
                END AS weather_category
            FROM silver.weather_clean
            ON CONFLICT (weather_id) DO NOTHING;
        """)

        conn.commit()
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

def traffic_accident_person():
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        # Insert data
        cur.execute("""
            INSERT INTO gold.traffic_accident_person (
                accident_id,
                sex,
                age,
                alcohol,
                injury_severity
            )
            SELECT
                accident_id,
                sex,
                age,
                alcohol,
                injury_severity
            FROM silver.traffic_accident_clean;
        """)

        conn.commit()
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

def fact_traffic_accidents():
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        # Insert data
        cur.execute("""
            INSERT INTO gold.fact_traffic_accidents (
                accident_id,
                city_id,
                time_id,
                road_id,
                weather_id,
                total_damage,
                main_cause
            )
            WITH accidents AS (
                SELECT
                    accident_id,
                    MIN(city) AS city,
                    MIN(lat) AS lat,
                    MIN(lon) AS lon,
                    MIN(accident_time) AS accident_time,
                    MIN(total_damage) AS total_damage,
                    MIN(main_cause) AS main_cause
                FROM silver.traffic_accident_clean
                GROUP BY accident_id
            )

            SELECT
                a.accident_id,
                c.city_id,
                t.time_id,
                r.road_id,
                w.weather_id,
                a.total_damage,
                a.main_cause

            FROM accidents a

            -- city
            LEFT JOIN gold.dim_city c
                ON a.city = c.city_name

            -- time
            LEFT JOIN gold.dim_time t
                ON DATE(a.accident_time) = t.date
                AND EXTRACT(HOUR FROM a.accident_time) = t.hour

            -- road (spatial)
            LEFT JOIN LATERAL (
                SELECT road_id
                FROM silver.osm_roads_clean r
                ORDER BY r.geom <-> ST_SetSRID(ST_Point(a.lon, a.lat), 4326)
                LIMIT 1
            ) r ON TRUE

            -- weather
            LEFT JOIN gold.dim_weather w
                ON w.date = DATE(a.accident_time)
                AND w.hour = date_trunc('hour', a.accident_time)::time

            ON CONFLICT (accident_id) DO NOTHING;
        """)

        conn.commit()
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)


# dim_city()
# dim_time()
# dim_road()
# dim_weather()
# fact_traffic_accidents()
# traffic_accident_person()