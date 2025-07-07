from airflow.exceptions import AirflowException, AirflowSkipException
from pyspark.sql import SparkSession
from datetime import timedelta
from pangres import upsert
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from helper.minio import MinioClient

import pandas as pd
import sys
import json
import ast

class Load:
    """
    A class used to load data into the staging area from various sources such as databases, APIs, and spreadsheets.
    """

    @staticmethod
    def _vcapital_db(table_name, incremental, date):
        """
        Load data from vcapital source database into staging area.

        Args:
            table_name (str): Name of the table to load data into.
            table_pkey (str): Primary key of the table.
            incremental (bool): Flag to indicate if the loading is incremental.
            date (str): Date string for the data to load.
        """
        spark = None
        try:
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName(f"Load to staging - {table_name}") \
                .getOrCreate()

            # Define bucket and object name
            bucket_name = 'extracted-data'
            
            if incremental:
                object_name = f"/vcapital_db/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime('%Y-%m-%d')}"
            else:
                object_name = f"/vcapital_db/{table_name}"

            try:
                # Read data from S3
                df = spark.read.parquet(f"s3a://{bucket_name}{object_name}")
            
            except:
                spark.stop()
                print(f"{table_name} doesn't have new data. Skipped...")
                return

            if df.rdd.isEmpty():
                spark.stop()
                print(f"{table_name} is empty. Skipped...")
                return

            # Load to staging
            db_url = "jdbc:postgresql://vcapital_db_dwh:5432/warehouse_db"
            db_properties = {
                "user": "postgres",
                "password": "postgres",
                "driver": "org.postgresql.Driver"
            }

            df.write.jdbc(
                url=db_url,
                table=f"staging.{table_name}",
                mode="append",
                properties=db_properties
            )
        
            print(f"Successfully loaded {table_name} using Spark JDBC.")
            
        except Exception as e:
            raise AirflowException(f"Error when loading {table_name}: {str(e)}")
        
        finally:
            if spark:
                spark.stop()

    @staticmethod
    def _vcapital_api(ds):
        """
        Load data from Dellstore API into staging area.

        Args:
            ds (str): Date string for the data to load.
        """
        bucket_name = 'extracted-data'
        object_name = f'/vcapital_api/data-{(pd.to_datetime(ds) - timedelta(days=1)).strftime("%Y-%m-%d")}.json'

        try:
            # Create SQLAlchemy engine
            engine = create_engine(PostgresHook(postgres_conn_id='warehouse-db').get_uri())

            try:
                # Get data from Minio
                minio_client = MinioClient._get()
                try:
                    data = minio_client.get_object(bucket_name=bucket_name, object_name=object_name).read().decode('utf-8')
                except:
                    raise AirflowSkipException(f"vcapital_api doesn't have new data. Skipped...")

                # Load data into Pandas DataFrame
                data = json.loads(data)
                df = pd.json_normalize(data)
                df = df.set_index('milestone_id')

                # Upsert data into database
                upsert(
                    con=engine,
                    df=df,
                    table_name='milestones',
                    schema='staging',
                    if_row_exists='update'
                )
                
            except AirflowSkipException as e:
                engine.dispose()
                raise e
            
            except Exception as e:
                engine.dispose()
                raise AirflowException(f"Error when loading data from venture capital API: {str(e)}")
            
        except AirflowSkipException as e:
            raise e
            
        except Exception as e:
            raise e
        
if __name__ == "__main__":
    """
    Main entry point for the script. Loads data into the staging area in warehose database.
    """
    if len(sys.argv) != 4:
        sys.exit(1)
        
    table_name = sys.argv[1]
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]
    
    Load._vcapital_db(table_name, incremental, date)
        