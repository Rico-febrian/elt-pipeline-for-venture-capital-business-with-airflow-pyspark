from airflow.decorators import dag
from datetime import datetime
from airflow.models.variable import Variable

# Asumsikan path ini benar sesuai struktur proyek Anda
from helper.callbacks.slack_notifier import slack_notifier 
from vcapital_warehouse.tasks.main import build_warehouse_pipeline

# Argumen default untuk DAG
default_args = {
    "owner": "Rico Febrian",
    "on_failure_callback": slack_notifier
}

@dag(
    dag_id="vcapital_warehouse",
    start_date=datetime(2024, 9, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["vcapital", "warehouse"],
    description="Extract, Transform and Load venture capital data into Warehouse"
)
def vcapital_warehouse_dag_runner():
    """
    DAG ini bertindak sebagai pemanggil atau 'runner'.
    Ia mengatur jadwal dan parameter, lalu memanggil logika utama.
    """
    try:
        # Ambil mode dari Airflow Variable, default ke False jika tidak ada
        incremental_mode = eval(Variable.get('vcapital_warehouse_incremental_mode', default_var='False'))
    except Exception:
        incremental_mode = False
    
    # Panggil fungsi yang membangun seluruh alur kerja dari file main.py
    build_warehouse_pipeline(incremental_mode=incremental_mode)

# Daftarkan DAG ini ke Airflow
vcapital_warehouse_dag_runner()