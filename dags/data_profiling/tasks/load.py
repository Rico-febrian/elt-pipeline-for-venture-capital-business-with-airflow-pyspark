from helper.minio import MinioClient
from helper.postgres import Execute
import pandas as pd

BASE_PATH = "/opt/airflow/dags"

class TransformLoad:
    def _vcapital_db():
        minio_client = MinioClient._get()
        bucket_name = 'data-profile-quality'
        object_name = f"/temp/vcapital_warehouse_db.csv"

        data = minio_client.get_object(
            bucket_name = bucket_name,
            object_name = object_name
        )

        df = pd.read_csv(data)

        df.insert(
            loc = 0, 
            column = "person_in_charge", 
            value = "Rico Febrian"
        )
        df.insert(
            loc = 1, 
            column = "source", 
            value = "warehouse_db"
        )

        Execute._insert_dataframe(
                connection_id = "data-profiling-db", 
                query_path = "/data_profiling/query/insert_profiling_result.sql",
                dataframe = df
        )