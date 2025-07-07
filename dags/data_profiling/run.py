from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from pendulum import datetime
from helper.postgres import Execute
from data_profiling.tasks.extract import Extract
from data_profiling.tasks.load import TransformLoad


@dag(
    dag_id = 'profiling_quality_pipeline',
    start_date = datetime(2024, 9, 1),
    schedule = "@daily",
    catchup = False
)
    
def data_profiling_pipeline():
    @task_group
    def vcapital_db():
        create_function = PythonOperator(
            task_id = 'create_function',
            python_callable = Execute._query,
            op_kwargs = {
                "connection_id": "warehouse-db",
                "query_path": "data_profiling/query/data_profiling_function.sql"
            }
        )
        
        extract = PythonOperator(
            task_id = 'extract',
            python_callable = Extract._vcapital_db
        )

        transform_load = PythonOperator(
            task_id = 'transform_and_load',
            python_callable = TransformLoad._vcapital_db
        )
  
        create_function >> extract >> transform_load

    vcapital_db()

data_profiling_pipeline()