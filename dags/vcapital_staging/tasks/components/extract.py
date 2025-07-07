from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.models import Variable
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from helper.minio import CustomMinio
from datetime import timedelta
import pandas as pd
import requests
import sys


class Extract:
    """
    A class used to extract data from various sources such as databases, APIs, and object storages.
    """

    @staticmethod
    def _vcapital_db(table_name, incremental, date):
        """
        Extract data from venture capital database.

        Args:
            table_name (str): Name of the table to extract data from.
            incremental (bool): Flag to indicate if the extraction is incremental.
            date (str): Date string for incremental extraction.

        Raises:
            AirflowException: If there is an error during extraction.
        """
        spark = None
        try:
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName(f"Extract Source DB - {table_name}") \
                .getOrCreate()
                
            # Define query and object name based on incremental flag
            query = f"(SELECT * FROM {table_name}) AS data"
            object_name = f"/vcapital_db/{table_name}"
            
            if incremental:
                query = f"(SELECT * FROM {table_name} WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY') AS data"
                object_name = f"/vcapital_db/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')}"    

            # Read data from source database
            df = spark.read.jdbc(
                url="jdbc:postgresql://vcapital_db_src:5432/vcapital_db_src",
                table=query,
                properties={
                    "user": "postgres",
                    "password": "postgres",
                    "driver": "org.postgresql.Driver"
                }
            )

            # Check if DataFrame is empty
            if df.isEmpty():
                print(f"{table_name} doesn't have new data. Skipped...")
                return None
            
            # Clean string columns from newline
            for col_name, col_type in df.dtypes:
                if col_type == 'string':
                    df = df.withColumn(col_name, regexp_replace(col(col_name), '\n', ' '))
            
            # Write DataFrame to MinIO
            bucket_name = "extracted-data"
            df.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(f"s3a://{bucket_name}/{object_name}")
            
        except Exception as e:
            raise AirflowException(f"Error when extracting {table_name}: {str(e)}")
        
        # Stop Spark session
        finally:
            if spark:
                spark.stop()

    @staticmethod
    def _vcapital_api(ds):
        """
        Extract data from venture capital API.

        Args:
            ds (str): Date string.

        Raises:
            AirflowException: If failed to fetch data from Dellstore API.
            AirflowSkipException: If no new data is found.
        """
        try:
            # Fetch data from API
            response = requests.get(
                url=Variable.get('vcapital_api_url'),
                params={"start_date": ds, "end_date": ds},
            )

            # Check response status
            if response.status_code != 200:
                raise AirflowException(f"Failed to fetch data from venture capital API. Status code: {response.status_code}")

            # Parse JSON data
            json_data = response.json()
            if not json_data:
                raise AirflowSkipException("No new data in venture capital API. Skipped...")

            # Replace newline characters in JSON data
            def replace_newlines(obj):
                if isinstance(obj, dict):
                    return {k: replace_newlines(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [replace_newlines(elem) for elem in obj]
                elif isinstance(obj, str):
                    return obj.replace('\n', ' ')
                else:
                    return obj

            json_data = replace_newlines(json_data)
            
            # Save JSON data to MiniO
            bucket_name = "extracted-data"
            file_date = (pd.to_datetime(ds) - timedelta(days=1)).strftime('%Y-%m-%d')
            object_name = f"/vcapital_api/data-{file_date}.json"
            CustomMinio._put_json(json_data, bucket_name, object_name)
            
        except AirflowSkipException as e:
            raise e
        
        except Exception as e:
            raise AirflowException(f"Error when extracting venture capital API: {str(e)}")


if __name__ == "__main__":
    """
    Main entry point for the script to extract data from the venture capital database or API.
    """
    if len(sys.argv) != 4:
        sys.exit(-1)

    table_name = sys.argv[1]    
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]

    Extract._vcapital_db(table_name, incremental, date)



