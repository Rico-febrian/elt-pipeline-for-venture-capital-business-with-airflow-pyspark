from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession
from datetime import timedelta
from sqlalchemy import create_engine
from pangres import upsert

import pandas as pd
import sys
import ast

class Load:
    """
    A class used to load data into the warehouse.
    """

    @staticmethod
    def _warehouse(table_name, incremental, date):
        """
        Load data into the warehouse.

        Args:
            table_name (str): The name of the table to load data into.
            table_pkey (str): The primary key of the table.
            incremental (bool): Flag to indicate if the process is incremental.
            date (str): The date for incremental loading.
        """
        spark = None
        try:
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName(f"Load to warehouse - {table_name}") \
                .getOrCreate()

            # Define bucket and object name
            bucket_name = 'transformed-data'
            object_name = f'/{table_name}'

            # Adjust object name for incremental loading
            if incremental:
                object_name = f'/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}'

            try:
                # Read data from S3
                df = spark.read.parquet(f"s3a://{bucket_name}{object_name}")
                
            except Exception as e:
                # Stop Spark session if reading data fails
                spark.stop()
                print(f"{table_name} doesn't have new data. Skipped...")
                raise

            # Check if DataFrame is empty
            if df.rdd.isEmpty():
                # Stop Spark session if DataFrame is empty
                spark.stop()
                print(f"{table_name} doesn't have new data. Skipped...")
                return

            # KONFIGURASI KONEKSI DATABASE
            db_url = "jdbc:postgresql://vcapital_db_dwh:5432/warehouse_db"
            db_properties = {
                "user": "postgres",
                "password": "postgres",
                "driver": "org.postgresql.Driver"
            }

            df.write.jdbc(
                url=db_url,
                table=f"warehouse.{table_name}",
                mode="append",
                properties=db_properties
            )
        
            print(f"Successfully loaded {table_name} using Spark JDBC.")
        
        except Exception as e:
            raise AirflowException(f"Error when loading {table_name}: {str(e)}")
        
        finally:
            if spark:
                # Stop Spark session
                spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: load.py <table_name> <incremental> <date>")
        sys.exit(-1)

    table_name = sys.argv[1]
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]

    Load._warehouse(
        table_name, 
        incremental, 
        date
    )