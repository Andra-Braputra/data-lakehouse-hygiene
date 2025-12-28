from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'andra',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 1, 1), # Sesuaikan ke tahun berjalan
}

# ======================================================
# DAG 1: API PIPELINE (Setiap 6 Jam)
# ======================================================
with DAG(
    dag_id='hygiene_api_pipeline',
    default_args=default_args,
    schedule_interval='0 */6 * * *', # Menit 0, setiap jam ke-0, 6, 12, 18
    catchup=False,
    tags=['6-hourly', 'api']
) as dag_api:

    ingest_bmkg = BashOperator(
        task_id='ingest_bmkg',
        bash_command='python /opt/airflow/ingestion/api_bmkg/bmkg_to_raw.py'
    )

    ingest_aqi = BashOperator(
        task_id='ingest_aqi',
        bash_command='python /opt/airflow/ingestion/api_aqicn/aqicn_to_raw.py'
    )

    clean_bmkg = BashOperator(
        task_id='clean_bmkg',
        bash_command='python /opt/airflow/elt/clean_bmkg.py'
    )

    clean_aqi = BashOperator(
        task_id='clean_aqi',
        bash_command='python /opt/airflow/elt/clean_aqicn.py'
    )

    ingest_bmkg >> clean_bmkg
    ingest_aqi >> clean_aqi


# ======================================================
# DAG 2: MAIN PIPELINE (Setiap 1 Jam)
# ======================================================
with DAG(
    dag_id='hygiene_main_pipeline',
    default_args=default_args,
    schedule_interval='0 * * * *', # Setiap jam (menit 0)
    catchup=False,
    tags=['hourly', 'main']
) as dag_main:

    # --- INGESTION ---
    ingest_sheets = BashOperator(
        task_id='ingest_sheets',
        bash_command='python /opt/airflow/ingestion/sheets/sheets_to_raw.py'
    )

    ingest_sql = BashOperator(
        task_id='ingest_sql',
        bash_command='python /opt/airflow/ingestion/sql/sql_to_raw.py'
    )

    # --- CLEANING ---
    clean_sheets = BashOperator(
        task_id='clean_sheets',
        bash_command='python /opt/airflow/elt/clean_sheets.py'
    )

    clean_sql = BashOperator(
        task_id='clean_sql',
        bash_command='python /opt/airflow/elt/clean_sql.py'
    )

    # --- LOGIC & LOADING ---
    # Skrip ini akan otomatis mengambil data Clean API terakhir yang tersedia di MinIO
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

    # --- FLOW ---
    ingest_sheets >> clean_sheets
    ingest_sql >> clean_sql

    # Prescriptive logic hanya butuh clean data dari Sheets & SQL untuk berjalan,
    # sementara data API diambil secara pasif dari state terakhir di MinIO.
    [clean_sheets, clean_sql] >> prescriptive_logic

    prescriptive_logic >> [load_activity, load_riwayat_mandi, load_result]
