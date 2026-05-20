from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param

import pendulum
from datetime import datetime, timedelta

# tasks
from scripts.traffic_accident_ingest import traffic_accident
from scripts.weather_ingest import weather_ingest
from scripts.osm_ingest import osm_ingest

from scripts.traffic_accident_clean import traffic_accident_data_clean
from scripts.weather_clean import weather_data_clean
from scripts.osm_clean import osm_data_clean

from scripts.gold_dim import city_dim_load, generate_date_dim, road_dim_load, weather_dim_load
from scripts.gold_fact import fact_traffic_accidents_load

# Define the local timezone
local_tz = pendulum.timezone("Europe/Prague")

# Default Args
default_args = {
    "owner": "barday",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "barday@email.cz",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2025, 12, 31, tzinfo=local_tz),
}

# ==========================================================
# Insert DAG
# ==========================================================
with DAG(
    dag_id='ingest_dag',
    default_args=default_args,
    description='DAG to ingest data from various sources',
    schedule=None,
    catchup=False,
    params={
        "backfill": Param(
            False, 
            type="boolean", 
            description="Run backfill?."
        )
    }
) as ingest_dag:
    
    # Define tasks
    ingest_traffic_accident_data = traffic_accident(backfill="{{ params.backfill }}")
    insert_weather_data = weather_ingest(backfill="{{ params.backfill }}")
    insert_osm_data = osm_ingest()

    trigger_clean = TriggerDagRunOperator(
        task_id='trigger_clean_dag',
        trigger_dag_id='clean_dag',
        wait_for_completion=False
    )

    # Define dependencies
    [ingest_traffic_accident_data, insert_weather_data, insert_osm_data] >> trigger_clean

# ==========================================================
# Clean DAG
# ==========================================================

with DAG(
    dag_id='clean_dag',
    default_args=default_args,
    description='DAG to clean ingested data',
    schedule=None,
    catchup=False
) as clean_dag:
    
    # Define tasks
    clean_traffic_accident_data = traffic_accident_data_clean()
    clean_weather_data = weather_data_clean("meteostat_raw")
    clean_osm_data = osm_data_clean("osm_ways")

    trigger_gold_dim = TriggerDagRunOperator(
        task_id='trigger_gold_dim_dag',
        trigger_dag_id='gold_dim_dag',
        wait_for_completion=False
    )

    # Define dependencies
    [clean_traffic_accident_data, clean_weather_data, clean_osm_data] >> trigger_gold_dim

# ==========================================================
# Gold DIM DAG
# ==========================================================

with DAG(
    dag_id='gold_dim_dag',
    default_args=default_args,
    description='DAG to load gold dimension tables',
    schedule=None,
    catchup=False
) as gold_dim_dag:
    
    # Define tasks
    load_city_dim = city_dim_load()
    generate_date_dimension = generate_date_dim()
    load_road_dim = road_dim_load("osm_roads_clean")
    load_weather_dim = weather_dim_load("weather_clean")

    trigger_gold_fact = TriggerDagRunOperator(
        task_id="trigger_gold_fact_dag",
        trigger_dag_id="gold_fact_dag",
        wait_for_completion=False
    )

    # Define dependencies
    load_city_dim >> generate_date_dimension >> load_road_dim >> load_weather_dim >> trigger_gold_fact

# ==========================================================
# Gold FACT DAG
# ==========================================================

with DAG(
    dag_id='gold_fact_dag',
    default_args=default_args,
    description='DAG to load gold fact tables',
    schedule=None,
    catchup=False
) as gold_fact_dag:
    
    # Define tasks
    load_fact_traffic_accidents = fact_traffic_accidents_load()

    # Define dependencies
    load_fact_traffic_accidents