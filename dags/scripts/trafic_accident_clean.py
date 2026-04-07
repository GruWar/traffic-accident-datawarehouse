from data_utils import connect_to_db, disconnect_from_db
import logging
from datetime import datetime
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)

def get_db_data(table_name, backfill=False):
    conn, cur = None, None
    try:
        conn, cur = connect_to_db()
        # Get data from db
        if backfill:
            cur.execute(f"""
                SELECT raw_id, payload FROM bronze.{table_name}
                ORDER BY raw_id DESC LIMIT 1;
            """)
            data = cur.fetchall()
        else:
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

def traffic_accident_data_clean(backfill=False):
    all_rows_to_insert = []
    
    try:
        # 1. Získání všech dat z bronze vrstvy
        # payload_records bude seznam tuplů: [(raw_id1, dict1), (raw_id2, dict2), ...]
        payload_records = get_db_data("traffic_accidents_raw")
        
        if not payload_records:
            logger.info("No data found in bronze layer.")
            return

        for record in payload_records: 
            raw_id = record['raw_id']
            full_geojson = record['payload']
            
            # 2. Procházení jednotlivých nehod (features) uvnitř jednoho GeoJSONu
            for feature in full_geojson.get('features', []):
                props = feature.get('properties', {})
                geom = feature.get('geometry', {})
                coords = geom.get('coordinates', [None, None])

                # Čištění hmotné škody
                raw_damage = props.get('hmotna_skoda', '0')
                property_damage = None
                if isinstance(raw_damage, str):
                    clean_damage = raw_damage.replace(' Kč', '').replace(' ', '')
                    if clean_damage.isdigit():
                        property_damage = int(clean_damage)
                elif isinstance(raw_damage, (int, float)):
                    property_damage = raw_damage

                # Zpracování data (přesunuto před definici row)
                datum_str = props.get('datum')
                formatted_date = None
                if datum_str:
                    try:
                        formatted_date = datetime.fromisoformat(datum_str).strftime('%Y-%m-%d')
                    except Exception:
                        formatted_date = None

                # 3. Sestavení finálního řádku pro Silver tabulku
                row = (
                    raw_id,
                    coords[1],  # lat
                    coords[0],  # lon
                    formatted_date,
                    props.get('pricina', '').strip() if props.get('pricina') else None,
                    props.get('druh'),
                    props.get('lehce_zraneno'),
                    props.get('tezce_zraneno'),
                    props.get('usmrceno'),
                    property_damage
                )
                all_rows_to_insert.append(row)

        # 4. Hromadný insert do Silver vrstvy
        if all_rows_to_insert:
            conn, cur = connect_to_db()
            execute_batch(cur, """
                INSERT INTO silver.traffic_accident_clean (
                    raw_id,
                    lat,
                    lon,
                    date,
                    cause, 
                    collision_type,
                    slightly_injured,
                    severely_injured, 
                    fatalities,
                    total_damage
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (raw_id) DO NOTHING;
            """, all_rows_to_insert)
            conn.commit()
            logger.info(f"Successfully cleaned and inserted {len(all_rows_to_insert)} records.")

    except Exception as e:
        logger.error(f"Error during cleaning process: {e}")
    finally:
        if 'conn' in locals() and conn:
            disconnect_from_db(conn, cur)


traffic_accident_data_clean()
