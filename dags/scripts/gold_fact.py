from scripts.data_utils import connect_to_db, disconnect_from_db
import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)

@task
def fact_traffic_accidents_load():
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        cur.execute(f"""
        INSERT INTO gold.fact_traffic_accidents (
            accident_id,
            city_id,
            date_id,
            road_id,
            weather_id,
            total_damage,
            main_cause
        )
        SELECT
            ta.accident_id,
            c.city_id,
            d.date_id,
            r.road_id,
            w.weather_id,
            ta.total_damage,
            ta.cause
        FROM silver.traffic_accident_clean AS ta

        -- find closest city
        CROSS JOIN LATERAL (
            SELECT c.city_id, c.station_id
            FROM gold.dim_city c
            ORDER BY 
                point(ta.lon, ta.lat) <-> point(c.lon, c.lat)
            LIMIT 1
        ) AS c

        -- date
        LEFT JOIN gold.dim_date AS d ON ta.date = d.date

        -- find closest road
        CROSS JOIN LATERAL (
            SELECT r.road_id
            FROM silver.osm_roads_clean r
            ORDER BY 
                -- ST_SetSRID vytvoří z lat/lon nehody PostGIS bod, který pak porovná s r.geom
                ST_SetSRID(ST_MakePoint(ta.lon, ta.lat), 4326) <-> r.geom
            LIMIT 1
        ) AS r

        -- weather
        LEFT JOIN gold.dim_weather AS w
        ON ta.date = w.date AND c.station_id = w.station_id
        ON CONFLICT (accident_id) DO NOTHING;
        """)
        conn.commit()
        logger.info(f"Successfully inserted data into gold.dim_weather")
    except Exception as e:
        logger.error(f"Error Inserting into database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)