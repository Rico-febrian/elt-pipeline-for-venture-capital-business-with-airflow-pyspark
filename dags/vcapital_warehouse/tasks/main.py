import pytz
from datetime import datetime

from airflow.decorators import task_group
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

# --- Konfigurasi dan Konstanta ---
# Semua konfigurasi Spark dan JAR dipindahkan ke sini
DATE = '{{ ds }}'

jar_list = [
    '/opt/spark/jars/hadoop-aws-3.3.1.jar',
    '/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar',
    '/opt/spark/jars/postgresql-42.2.23.jar'
]

# Define Spark configuration
spark_conf = {
    'spark.hadoop.fs.s3a.access.key': 'minio',
    'spark.hadoop.fs.s3a.secret.key': 'minio123',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.dynamicAllocation.enabled': 'true',
    'spark.dynamicAllocation.maxExecutors': '3',
    'spark.dynamicAllocation.minExecutors': '1',
    'spark.dynamicAllocation.initialExecutors': '1',
    'spark.executor.memory': '2g',  # Define RAM per executor
    'spark.executor.cores': '2',  # Define cores per executor
    'spark.scheduler.mode': 'FAIR'
}

# --- Pemisahan Konfigurasi untuk Dimensi dan Fakta ---
dim_transform_configs = {
    'company': "vcapital_db.load.company",
    'funds': "vcapital_db.load.funds",
}
dim_load_configs = ['dim_company', 'dim_funds']

fact_transform_configs = {
    'investments': "vcapital_db.load.investments",
    'acquisition': "vcapital_db.load.acquisition",
    'ipos': "vcapital_db.load.ipos",
}
fact_load_configs = [ 'fct_investments', 'fct_acquisition', 'fct_ipos']

# Fungsi untuk ExternalTaskSensor
def target_execution_date(execution_date, **context):
    return datetime(2004, 2, 28, tzinfo=pytz.UTC)

# --- Fungsi Utama yang Akan Diimpor oleh DAG ---
def build_warehouse_pipeline(incremental_mode: bool):
    """
    Fungsi ini membangun alur kerja ELT untuk warehouse.
    Ini akan dipanggil oleh file DAG utama.
    """

    @task_group(group_id="process_dimensions")
    def process_dimensions_group():
        transform_tasks = []
        for table_name, dependency in dim_transform_configs.items():
            wait = ExternalTaskSensor(task_id=f'wait_staging_{table_name}', external_dag_id='vcapital_staging', external_task_id=dependency)
            transform = SparkSubmitOperator(
                task_id=f'transform_{table_name}',
                conn_id='spark-conn',
                application=f'dags/vcapital_warehouse/tasks/components/extract_transform.py',
                application_args=[f'{table_name}', f'{incremental_mode}', DATE],
                conf=spark_conf, jars=','.join(jar_list)
            )
            wait >> transform
            transform_tasks.append(transform)

        load_tasks = []
        for table in dim_load_configs:
            load = SparkSubmitOperator(
                task_id=f'load_{table}',
                conn_id='spark-conn',
                application=f'dags/vcapital_warehouse/tasks/components/load.py',
                application_args=[f'{table}', f'{incremental_mode}', DATE],
                conf=spark_conf, jars=','.join(jar_list)
            )
            load_tasks.append(load)
        
        transform_tasks >> load_tasks[0]
        for i in range(len(load_tasks) - 1):
            load_tasks[i] >> load_tasks[i + 1]

    @task_group(group_id="process_facts")
    def process_facts_group():
        transform_tasks = []
        for table_name, dependency in fact_transform_configs.items():
            wait = ExternalTaskSensor(task_id=f'wait_staging_{table_name}', external_dag_id='vcapital_staging', external_task_id=dependency)
            transform = SparkSubmitOperator(
                task_id=f'transform_{table_name}',
                conn_id='spark-conn',
                application=f'dags/vcapital_warehouse/tasks/components/extract_transform.py',
                application_args=[f'{table_name}', f'{incremental_mode}', DATE],
                conf=spark_conf, jars=','.join(jar_list)
            )
            wait >> transform
            transform_tasks.append(transform)

        load_tasks = []
        for table in fact_load_configs:
            load = SparkSubmitOperator(
                task_id=f'load_{table}',
                conn_id='spark-conn',
                application=f'dags/vcapital_warehouse/tasks/components/load.py',
                application_args=[f'{table}', f'{incremental_mode}', DATE],
                conf=spark_conf, jars=','.join(jar_list)
            )
            load_tasks.append(load)
        
        transform_tasks >> load_tasks[0]
        for i in range(len(load_tasks) - 1):
            load_tasks[i] >> load_tasks[i + 1]

    # Atur rantai dependensi utama
    process_dimensions_group() >> process_facts_group()
