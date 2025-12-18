from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'andra',
    'retries': 0,
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='hygiene_lakehouse_pipeline',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False
) as dag:

    # --- GROUP 1: INGESTION ---
    ingest_sheets = BashOperator(
        task_id='ingest_sheets',
        bash_command='python /opt/airflow/ingestion/sheets/sheets_to_raw.py'
    )

    ingest_bmkg = BashOperator(
        task_id='ingest_bmkg',
        bash_command='python /opt/airflow/ingestion/api_bmkg/bmkg_to_raw.py'
    )

    ingest_aqi = BashOperator(
        task_id='ingest_aqi',
        bash_command='python /opt/airflow/ingestion/api_aqicn/aqicn_to_raw.py'
    )

    ingest_sql = BashOperator(
        task_id='ingest_sql',
        bash_command='python /opt/airflow/ingestion/sql/sql_to_raw.py'
    )

    # --- GROUP 2: CLEANING ---
    clean_sheets = BashOperator(
        task_id='clean_sheets',
        bash_command='python /opt/airflow/elt/clean_sheets.py'
    )

    clean_bmkg = BashOperator(
        task_id='clean_bmkg',
        bash_command='python /opt/airflow/elt/clean_bmkg.py'
    )

    clean_aqi = BashOperator(
        task_id='clean_aqi',
        bash_command='python /opt/airflow/elt/clean_aqicn.py'
    )

    clean_sql = BashOperator(
        task_id='clean_sql',
        bash_command='python /opt/airflow/elt/clean_sql.py'
    )

    # --- GROUP 3: LOGIC & LOADING ---
    prescriptive_logic = BashOperator(
        task_id='prescriptive_logic',
        bash_command='python /opt/airflow/elt/prescriptive/prescriptive_logic.py'
    )

    load_activity = BashOperator(
        task_id='load_activity_to_neon',
        bash_command='python /opt/airflow/elt/load/load_aktivitas_to_neon.py'
    )

    load_riwayat_mandi = BashOperator(
        task_id='load_riwayat_mandi_to_neon',
        bash_command='python /opt/airflow/elt/load/load_riwayat_mandi_to_neon.py'
    )

    load_result = BashOperator(
        task_id='load_result_to_neon',
        bash_command='python /opt/airflow/elt/load/load_prescriptive_to_sql.py'
    )

    # --- ALUR DEPENDENCY (FLOW) ---
    ingest_sheets >> clean_sheets
    ingest_bmkg >> clean_bmkg
    ingest_aqi >> clean_aqi
    ingest_sql >> clean_sql

    [clean_sheets, clean_bmkg, clean_aqi, clean_sql] >> prescriptive_logic

    prescriptive_logic >> [load_activity, load_riwayat_mandi, load_result]