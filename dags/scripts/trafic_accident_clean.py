from data_utils import connect_to_db, disconnect_from_db
import logging
from datetime import datetime
from psycopg2.extras import execute_batch

logger = logging.getLogger(__name__)

def get_db_data(cur, table_name, backfill=False):
    try:
        if backfill:
            query = f"SELECT raw_id, payload FROM bronze.{table_name}"
        else:
            query = f"SELECT raw_id, payload FROM bronze.{table_name} ORDER BY raw_id DESC LIMIT 1"
        
        cur.execute(query)
        return cur
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        return None

def traffic_accident_data_clean(backfill=False):
    all_rows_to_insert = []
    conn_read, cur_read = None, None
    
    try:
        conn_read, cur_read = connect_to_db()
        records_stream = get_db_data(cur_read, "traffic_accidents_raw", backfill)

        if not records_stream:
            return

        for record in records_stream:
            # Zpracování recordu (tuple vs dict)
            try:
                raw_id = record['raw_id']
                payload = record['payload']
            except (TypeError, KeyError):
                raw_id = record[0]
                payload = record[1]

            # Ošetření listu v payloadu
            if isinstance(payload, list) and len(payload) > 0:
                full_geojson = payload[0]
            else:
                full_geojson = payload
            
            if not isinstance(full_geojson, dict):
                continue

            # OPRAVA CYKLU: feature.get('features')
            features = full_geojson.get('features', [])
            
            for feature in features:
                if not isinstance(feature, dict):
                    continue
                
                props = feature.get('properties', {})
                geom = feature.get('geometry', {})
                coords = geom.get('coordinates', [None, None])

                # Clean damage
                raw_damage = props.get('hmotna_skoda', 0)
                property_damage = None
                if isinstance(raw_damage, str):
                    clean_damage = raw_damage.replace(' Kč', '').replace(' ', '')
                    if clean_damage.isdigit():
                        property_damage = int(clean_damage)
                elif isinstance(raw_damage, (int, float)):
                    property_damage = raw_damage

                # Date formatting
                datum_str = props.get('datum')
                formatted_date = None
                if datum_str:
                    try:
                        formatted_date = datetime.fromisoformat(datum_str).strftime('%Y-%m-%d')
                    except:
                        pass
                
                row = (
                    raw_id, coords[1], coords[0], formatted_date,
                    props.get('pricina', '').strip() if props.get('pricina') else None,
                    props.get('druh'), props.get('lehce_zraneno'),
                    props.get('tezce_zraneno'), props.get('usmrceno'),
                    property_damage
                )
                all_rows_to_insert.append(row)


        if all_rows_to_insert:
            print(f"Připraveno {len(all_rows_to_insert)} řádků. Zahajuji zápis...")
            conn_write, cur_write = connect_to_db()
            try:
                execute_batch(cur_write, """
                    INSERT INTO silver.traffic_accident_clean (
                        raw_id, lat, lon, date, cause, 
                        collision_type, slightly_injured, severely_injured, 
                        fatalities, total_damage
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, all_rows_to_insert, page_size=5000)
                conn_write.commit()
                print("Data úspěšně uložena do Silver vrstvy.")
            finally:
                disconnect_from_db(conn_write, cur_write)
        else:
            print("Žádná data k uložení nenalezena.")

    except Exception as e:
        logger.error(f"Error during cleaning process: {e}")
    finally:
        if conn_read:
            disconnect_from_db(conn_read, cur_read)

traffic_accident_data_clean(backfill=True)
