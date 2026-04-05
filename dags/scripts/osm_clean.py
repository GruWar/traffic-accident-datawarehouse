from data_utils import connect_to_db, disconnect_from_db
from psycopg2.extras import execute_batch
import logging
import json

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


def osm_data_clean():
    conn, cur = None, None

    try:
        conn, cur = connect_to_db()

        records = get_db_data("osm_raw")

        data = []

        for record in records:
            payload = record["payload"]

            # pokud je payload string → parse
            if isinstance(payload, str):
                payload = json.loads(payload)

            elements = payload.get("elements", [])

            for element in elements:
                if element.get("type") != "way":
                    continue

                tags = element.get("tags", {})

                # --- základní atributy ---
                name = tags.get("name")
                road_type = tags.get("highway")

                lanes = tags.get("lanes")
                lanes = int(lanes) if lanes and lanes.isdigit() else None

                max_speed = tags.get("maxspeed")
                max_speed = int(max_speed) if max_speed and max_speed.isdigit() else None

                oneway = tags.get("oneway")
                if oneway == "yes":
                    oneway = True
                elif oneway == "no":
                    oneway = False
                else:
                    oneway = None

                is_intersection = None  # zatím neřešíme

                # --- geometry ---
                points = element.get("geometry", [])
                if not points or len(points) < 2:
                    continue

                coords = [
                    f"{p['lon']} {p['lat']}"
                    for p in points
                ]

                linestring_wkt = f"LINESTRING({', '.join(coords)})"

                data.append((
                    name,
                    road_type,
                    is_intersection,
                    lanes,
                    max_speed,
                    oneway,
                    linestring_wkt
                ))

        # --- INSERT ---
        execute_batch(cur, """
            INSERT INTO silver.osm_roads_clean (
                name,
                road_type,
                is_intersection,
                lanes,
                max_speed,
                oneway,
                geom
            )
            VALUES (%s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))
        """, data)

        conn.commit()
        logger.info(f"Inserted {len(data)} OSM roads")

    except Exception:
        logger.exception("Error while processing OSM data")

    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)

osm_data_clean()