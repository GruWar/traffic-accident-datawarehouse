from data_utils import connect_to_db, disconnect_from_db
import logging

logger = logging.getLogger(__name__)

def osm_data_clean(table_name):
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        cur.execute(f"""
             INSERT INTO silver.osm_roads_clean (
                road_id,
                name,
                road_type,
                is_intersection,
                lanes,
                max_speed,
                oneway,
                geom
            )
            SELECT
                osm_id AS road_id,
                tags->'name' AS name,
                tags->'highway' AS road_type,
                false AS is_intersection,
                abs((tags->'lanes')::int) AS lanes,
                CASE
                    WHEN tags->'maxspeed' = 'CZ:urban' THEN 50
                    WHEN tags->'maxspeed' = 'CZ:rural' THEN 90
                    WHEN tags->'maxspeed' = 'CZ:living_street' THEN 20
                    WHEN tags->'maxspeed' = 'CZ:pedestrian_zone' THEN 20
                    WHEN tags->'maxspeed' = 'walk' THEN 5
                    WHEN tags->'maxspeed' = 'none' THEN NULL
                    
                    WHEN tags->'maxspeed' LIKE '%mph' THEN 
                    (REPLACE(tags->'maxspeed', ' mph', '')::NUMERIC * 1.609)::int
                    WHEN substring(tags->'maxspeed' from '[0-9]+') IS NOT NULL THEN 
                    substring(tags->'maxspeed' from '[0-9]+')::int
                    WHEN tags->'highway' = 'residential' THEN 30
                    WHEN tags->'highway' = 'motorway' THEN 130
                    ELSE NULL
                END AS max_speed,
                CASE
                    WHEN tags->'oneway' IN ('yes', 'alternating', 'reversible', '1', '-1') THEN true
                    WHEN tags->'oneway' IN ('no', '0') THEN false
                    ELSE false
                END AS oneway,
                geom
            FROM bronze.{table_name}
        """)
        conn.commit()
        logger.info(f"Successfully inserted data into silver.osm_roads_clean")
    except Exception as e:
        logger.error(f"Error Inserting into database: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

osm_data_clean("osm_ways")