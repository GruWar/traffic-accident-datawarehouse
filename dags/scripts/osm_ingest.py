import os
import duckdb
import logging
import requests
from data_utils import connect_to_db, disconnect_from_db
from psycopg2.extras import execute_values

# Nastavení logování
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_osm_config(path):
    """Vytvoří základní osmconf.ini, aby DuckDB vědělo, jak parsovat tagy."""
    config_content = """
[lines]
osm_id=yes
osm_version=no
osm_timestamp=no
attributes=highway,name,maxspeed,lanes,oneway,junction
all_tags=yes
"""
    with open(path, "w") as f:
        f.write(config_content)
    logger.info(f"Konfigurační soubor vytvořen: {path}")

def download_osm_data(url, target_folder, filename):
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        logger.info(f"Složka vytvořena: {target_folder}")

    file_path = os.path.join(target_folder, filename)

    if os.path.exists(file_path):
        os.remove(file_path)
        logger.info(f"Starý soubor {filename} byl odstraněn.")

    logger.info(f"Zahajuji stahování z: {url}")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=65536):
                    f.write(chunk)
        
        logger.info(f"Staženo: {file_path} ({os.path.getsize(file_path) // (1024*1024)} MB)")
        return file_path
    except Exception as e:
        logger.error(f"Chyba při stahování: {e}")
        return None

def osm_ingest(new_data=False):
    pg_conn, pg_cur = None, None
    URL_ADDRESS = "https://download.geofabrik.de/europe/czech-republic-latest.osm.pbf"
    TARGET_FOLDER = "data/osm"
    FILENAME = "czech-republic-latest.osm.pbf"
    CONFIG_NAME = "osmconf.ini"

    # Cesty k souborům
    pbf_path = os.path.abspath(os.path.join(TARGET_FOLDER, FILENAME))
    config_path = os.path.abspath(os.path.join(TARGET_FOLDER, CONFIG_NAME))

    # 1. DOWNLOAD LOGIKA
    if new_data or not os.path.exists(pbf_path):
        logger.info("Soubor chybí nebo je vynuceno stažení.")
        download_res = download_osm_data(URL_ADDRESS, TARGET_FOLDER, FILENAME)
        if not download_res:
            logger.error("Nepodařilo se získat zdrojová data. Ukončuji.")
            return
    else:
        logger.info(f"Používám stávající soubor: {pbf_path}")

    # 2. CONFIG LOGIKA (GDAL workaround)
    create_osm_config(config_path)
    os.environ["OSM_CONFIG_FILE"] = config_path

    try:
        # Připojení k Postgresu
        pg_conn, pg_cur = connect_to_db()
        
        # DuckDB nastavení
        ddb = duckdb.connect()
        ddb.execute("SET memory_limit = '4GB';") # Prevence přetečení paměti na Windows
        ddb.execute("INSTALL spatial; LOAD spatial;")

        logger.info("Konvertuji PBF přes DuckDB (Interleaved Reading zapnuto)...")
        
        # SQL cesta pro DuckDB (musí mít dopředná lomítka)
        sql_pbf_path = pbf_path.replace("\\", "/")
        
        # ST_Read s parametry layer='lines' a INTERLEAVED_READING=YES
        query = f"""
            CREATE OR REPLACE TEMP TABLE temp_roads AS 
            SELECT 
                osm_id, 
                ST_AsWKB(geom) AS geom_wkb, 
                to_json(all_tags) AS json_tags  -- Zde je ta změna
            FROM st_read(
                '{sql_pbf_path}', 
                layer='lines', 
                open_options=['INTERLEAVED_READING=YES']
            )
            WHERE highway IS NOT NULL;
        """
        ddb.execute(query)
        
        # Přenos do DataFrame
        df = ddb.execute("SELECT osm_id, geom_wkb, json_tags FROM temp_roads").fetchdf()
        logger.info(f"Načteno {len(df)} řádků silnic. Zahajuji zápis do Postgresu...")

        # 3. ZÁPIS DO POSTGRESU (Bronze vrstva)
        # Tabulka v DDL: bronze.osm_ways (osm_id, geometry, tags)
        pg_cur.execute("TRUNCATE TABLE bronze.osm_ways;")
        
        insert_query = """
            INSERT INTO bronze.osm_ways (osm_id, geom, tags)
            VALUES %s
        """
        
        # Transformace dat pro psycopg2
        data_to_insert = [
            (
                int(row['osm_id']), 
                row['geom_wkb'], 
                row['json_tags']  # Toto už je díky to_json() validní JSON string
            ) 
            for _, row in df.iterrows()
        ]
        
        # Nahrávání po dávkách
        execute_values(pg_cur, insert_query, data_to_insert, 
                      template="(%s, ST_GeomFromWKB(%s), %s)",
                      page_size=10000)
        
        pg_conn.commit()
        logger.info("Import do Bronze vrstvy byl úspěšně dokončen!")

    except Exception as e:
        logger.error(f"Chyba během ingestu: {e}")
        if pg_conn:
            pg_conn.rollback()
    finally:
        if pg_conn:
            disconnect_from_db(pg_conn, pg_cur)
        if 'ddb' in locals():
            ddb.close()

if __name__ == "__main__":
    osm_ingest(new_data=False)