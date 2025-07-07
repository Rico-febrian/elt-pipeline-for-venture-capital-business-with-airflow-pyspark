from pyspark.sql import SparkSession, DataFrame
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import pyspark
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def extract_warehouse(spark: SparkSession, table_name: str, schema: str) -> DataFrame:
    """
    Extract a table from the warehouse database via JDBC using Spark.

    Args:
        spark (SparkSession): Spark session object.
        table_name (str): Name of the table to extract (e.g. "staging.company" or "dim_date").

    Returns:
        DataFrame: Spark DataFrame containing the data from the warehouse table.
    """
    try:
        # Get Postgres connection from Airflow
        hook = PostgresHook(postgres_conn_id='warehouse-db')
        conn = hook.get_connection(hook.postgres_conn_id)

        # Compose JDBC connection string and properties
        url = f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}"
        properties = {
            "user": conn.login,
            "password": conn.password,
            "driver": "org.postgresql.Driver"
        }
        
        table = f"{schema}.{table_name}"

        # Read the table into Spark DataFrame
        df = spark.read.jdbc(url=url, table=table, properties=properties)
        return df

    except Exception as e:
        print(f"[ERROR] Failed to extract {table_name} from warehouse: {str(e)}")
        return None
    

def clean_address(col_name: str):
    """
    Cleans address values in a DataFrame column through the following steps:
    1. Removing special characters '#' or '.' at the beginning of the string.
    2. Converting the entire string to lowercase for standardization.
    3. Identifying and replacing potentially invalid values with NULL.
       A value is considered invalid if it consists solely of symbols and/or numbers,
       or if its length after trimming leading and trailing spaces is less than or equal to 2 characters.

    Parameters:
        col_name (str): The name of the column containing the address values to clean.

    Returns:
        Column: A PySpark Column containing the cleaned address values.
                Invalid values will be replaced with NULL.
    """

    # Step 1: Convert to lowercase and remove '#' or '.' characters at the start of the string.
    # Example: '#Main St' becomes 'main st', '.Apartment 1A' becomes 'apartment 1a'
    cleaned = F.regexp_replace(F.lower(F.col(col_name)), r"^[#.]+", "")

    # Step 2: Define conditions to identify invalid values.

    # Condition 1: Check if the value (after step 1) consists solely of non-word characters
    #              (symbols, spaces, punctuation), digits (numbers), or underscores.
    #              Examples of values considered invalid: '??', '.323' (after removal becomes '323'), '------', ' !? '
    is_only_symbols = cleaned.rlike(r"^[\W\d_]+$")

    # Condition 2: Check if the length of the value (after step 1 and trimming) is too short.
    #              Values with a length of 2 characters or less after trimming are considered invalid.
    #              Examples of values considered invalid: 'a', ' b ', ''
    is_too_short = F.length(F.trim(cleaned)) <= 2

    # Step 3: Apply the cleaning logic.
    # If a value meets either of the invalid conditions (only symbols or too short),
    # then replace it with NULL. Otherwise, return the cleaned and trimmed value.
    cleaned_data = F.when(
        is_only_symbols | is_too_short,
        F.lit(None)  # Replace invalid values with NULL
    ).otherwise(
        F.trim(cleaned)  # Keep and trim valid values
    )

    return cleaned_data


def to_usd(currency_col, amount_col):
    """
    Converts various currency amounts to USD based on predefined exchange rates.

    Args:
        currency_col (_type_): _description_
        amount_col (_type_): _description_

    Returns:
        _type_: _description_
    """

    exchange_rate = F.round(
        F.when(F.col(currency_col) == "USD", F.col(amount_col))
         .when(F.col(currency_col) == "CAD", F.col(amount_col) * 0.72)
         .when(F.col(currency_col) == "EUR", F.col(amount_col) * 1.14)
         .when(F.col(currency_col) == "SEK", F.col(amount_col) * 0.10)
         .when(F.col(currency_col) == "AUD", F.col(amount_col) * 0.64)
         .when(F.col(currency_col) == "JPY", F.col(amount_col) * 0.007)
         .when(F.col(currency_col) == "GBP", F.col(amount_col) * 1.33)
         .when(F.col(currency_col) == "NIS", F.col(amount_col) * 0.28)
         .otherwise(F.col(amount_col)),
        2
    )

    return exchange_rate