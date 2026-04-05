from data_utils import connect_to_db, disconnect_from_db
import logging
from pyproj import Transformer
from datetime import datetime
from psycopg2.extras import execute_batch

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

def traffic_accident_data_clean():
    conn, cur = None, None
    data = []
    try:
        transformer = Transformer.from_crs("EPSG:5514", "EPSG:4326")
        # Get data
        payload = get_db_data("traffic_accidents_raw")
        for record in payload:
            # Clean data
            # Alcohol field is "ano" or "ne", we want to convert it to boolean
            val = record["payload"].get("alkohol")
            if val == "Ano":
                alcohol = True
            elif val == "Ne":
                alcohol = False
            else:
                alcohol = None
            
            # lat and lon
            lat, lon = transformer.transform(record["payload"]["x"], record["payload"]["y"])

            # date
            date_part = datetime.strptime(
                record["payload"]["datum"],
                "%m/%d/%Y %I:%M:%S %p"
            )

            try:
                cas = str(record["payload"].get("cas", "0000")).zfill(4)
                hour = int(cas[:2])
                minute = int(cas[2:])

                if not (0 <= hour < 24 and 0 <= minute < 60):
                    raise ValueError

            except:
                hour, minute = 0, 0

            dt = date_part.replace(hour=hour, minute=minute, second=0)

            row = [
                record["raw_id"],
                record["payload"]["id_nehody"],
                "Brno",
                record["payload"]["katastr"],
                lat,
                lon,
                dt,
                record["payload"]["hlavni_pricina"],
                record["payload"]["tezce_zran_os"],
                record["payload"]["stav_vozovky"],
                record["payload"]["povetrnostni_podm"],
                record["payload"]["viditelnost"],
                record["payload"]["druh_vozidla"],
                record["payload"]["osoba"],
                record["payload"]["pohlavi"],
                record["payload"]["vek"],
                alcohol,
                record["payload"]["hmotna_skoda"]
            ]
            data.append(row)

        # insert into db
        conn, cur = connect_to_db()
        execute_batch(cur, """
            INSERT INTO silver.traffic_accident_clean (
                raw_id,
                accident_id,
                city,
                district,
                lat,
                lon,
                accident_time,
                main_cause,
                injury_severity,
                road_condition,
                weather_condition,
                visibility,
                vehicle_type,
                person_type,
                sex,
                age,
                alcohol,
                total_damage)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, 'N/A'), %s, %s, %s);
            """, data)
        conn.commit()
    except Exception as e:
        logger.error(f"Error while inserting: {e}")
    finally:
        if conn and cur:
            disconnect_from_db(conn, cur)


traffic_accident_data_clean()
