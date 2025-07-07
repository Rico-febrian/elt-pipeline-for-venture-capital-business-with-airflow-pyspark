from airflow.decorators import dag
from vcapital_staging.tasks.extract_db import vcapital_db
from vcapital_staging.tasks.extract_api import vcapital_api
from datetime import datetime
from helper.callbacks.slack_notifier import slack_notifier
from airflow.models.variable import Variable

default_args = {
    "owner": "Rico Febrian",
    "on_failure_callback": slack_notifier
}

@dag(
    dag_id="vcapital_staging",
    start_date=datetime(2024, 9, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["vcapital", "staging"],
    description="Extract, and Load venture capital data into Staging area"
)

def vcapital_staging_dag():
    incremental_mode = eval(Variable.get('vcapital_staging_incremental_mode'))
    # Run the dellstore_db task with the incremental flag
    db_task = vcapital_db(incremental=incremental_mode)
    
    # Run the dellstore_api task
    api_task = vcapital_api()
    
    # Set the task dependencies
    [db_task, api_task]

vcapital_staging_dag()