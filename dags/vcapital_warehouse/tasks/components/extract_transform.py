from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid
from helper.transform import clean_address, extract_warehouse, to_usd

import sys

# Define paths for transformed, valid, and invalid data
transformed_data_path = 's3a://transformed-data/'

# Define PostgreSQL connection properties
postgres_staging = "jdbc:postgresql://vcapital_db_src:5432/vcapital_db_src"
postgres_warehouse = "jdbc:postgresql://vcapital_db_dwh:5432/warehouse_db"
postgres_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

class ExtractTransform:
    """
    A class used to extract and transform data from staging area to warehouse.
    """
    @staticmethod
    def _company(incremental, date):
        """
        Extracts and transforms company table from the staging area to the warehouse.

        Args:
            incremental (bool): Whether to load only today's data.
            date (str): Execution date passed from Airflow (format: YYYY-MM-DD)
        """
        spark = None
        try: 
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform from Staging - Company") \
                .getOrCreate()
                
            # Define query for extracting data
            query = "(SELECT * FROM staging.company) AS data"
            if incremental:
                query = f"(SELECT * FROM staging.company WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 day') AS data"
                
            # Read data from PostgreSQL staging area
            df = spark.read.jdbc(
                url = postgres_warehouse,
                table = query,
                properties = postgres_properties
            ) 
            
            # Check if DataFrame is empty
            if df.isEmpty():
                spark.stop()
                print("No data to process for company table.")
                return
            
            # Transform data
            
            # Add a new column 'entity_type' to identify if the record is a 'company' or a 'fund'
            # based on the 'object_id' prefix.
            df = df.withColumn(
                "entity_type",
                F.when(F.col("object_id").startswith("c:"), "company")
                .when(F.col("object_id").startswith("f:"), "fund")
                .otherwise(None)
            )

            # Create cleaned versions of address columns by applying the 'clean_address' function.
            df = df.withColumn("address1_cleaned", clean_address(col_name="address1")) \
                .withColumn("address2_cleaned", clean_address(col_name="address2"))

            # Create the 'full_address' column by concatenating 'address1_cleaned' and 'address2_cleaned'.
            # It handles cases where one or both address columns are null or empty.
            df = df.withColumn(
                "full_address",
                F.when(
                    (F.col("address1_cleaned").isNull()) & (F.col("address2_cleaned").isNull()),
                    F.lit(None)
                ).when(
                    (F.col("address1_cleaned").isNull()) | (F.col("address1_cleaned") == ""),
                    F.col("address2_cleaned")
                ).when(
                    (F.col("address2_cleaned").isNull()) | (F.col("address2_cleaned") == ""),
                    F.col("address1_cleaned")
                ).otherwise(
                    F.concat_ws(", ", F.col("address1_cleaned"), F.col("address2_cleaned"))
                )
            )

            # Standardize the values for 'region', 'city', and 'country_code' by:
            # 1. Trimming leading/trailing whitespace.
            # 2. Converting 'region' and 'city' to lowercase.
            # 3. Converting 'country_code' to uppercase.
            region_cleaned = F.trim(F.lower(F.col("region")))
            city_cleaned = F.trim(F.lower(F.col("city")))
            country_code_cleaned = F.trim(F.upper(F.col("country_code")))

            # Update the 'region', 'city', and 'country_code' columns with the cleaned values,
            # setting them to null if the cleaned value is null or empty.
            df = df.withColumn(
                "region",
                F.when(
                    (region_cleaned.isNull()) | (region_cleaned == ""), F.lit(None)
                ).otherwise(region_cleaned)
            ).withColumn(
                "city",
                F.when(
                    (city_cleaned.isNull()) | (city_cleaned == ""), F.lit(None)
                ).otherwise(city_cleaned)
            ).withColumn(
                "country_code",
                F.when(
                    (country_code_cleaned.isNull()) | (country_code_cleaned == ""), F.lit(None)
                ).otherwise(country_code_cleaned)
            )

            # Select the necessary columns for the dimension table and rename 'object_id' to 'nk_company_id'
            # as the natural key.
            dim_company = df.select(
                F.col("object_id").alias("nk_company_id"),
                F.col("entity_type"),
                F.col("full_address"),
                F.col("region"),
                F.col("city"),
                F.col("country_code")
            )
            
            # Write the transformed data to MinIO
            dim_company.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(f"{transformed_data_path}/dim_company")
                
        except Exception as e:
            raise AirflowException(f"Error processing company table: {str(e)}")
        
        finally:
            if spark:
                spark.stop()
                print("Spark session stopped.")


    @staticmethod
    def _funds(incremental, date):
        """
        Extracts and transforms funds table from the staging area to the warehouse.

        Args:
            incremental (bool): Whether to load only today's data.
            date (str): Execution date passed from Airflow (format: YYYY-MM-DD)
        """
        spark = None
        try: 
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform from Staging - Funds") \
                .getOrCreate()
                
            # Define query for extracting data
            query = "(SELECT * FROM staging.funds) AS data"
            if incremental:
                query = f"(SELECT * FROM staging.funds WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 day') AS data"
                
            # Read data from PostgreSQL staging area
            df = spark.read.jdbc(
                url = postgres_warehouse,
                table = query,
                properties = postgres_properties
            ) 
            
            # Check if DataFrame is empty
            if df.isEmpty():
                spark.stop()
                print("No data to process for company table.")
                return
            
            # Transform data
            
            # Extract the dim_date dimension table.
            dim_date = extract_warehouse(spark, table_name="dim_date", schema="warehouse")

            # Standardize 'name' and 'source_description' by trimming whitespace and converting to lowercase.
            df = df.withColumn("name", F.trim(F.lower(F.col("name")))) \
                .withColumn("source_description", F.trim(F.lower(F.col("source_description"))))
            
            # Convert raised amount to USD using the to_usd function.
            df = df.withColumn("raised_amount_usd", to_usd(currency_col="raised_currency_code", amount_col="raised_amount"))

            # Add a foreign key 'funded_date_id' by formatting 'funded_at' to match the 'date_id' in dim_date.
            df = df.withColumn(
                "funded_date_id",
                F.date_format(df.funded_at, "yyyyMMdd").cast("integer")
            )

            # Join with dim_date to get date information based on 'funded_date_id'.
            df = df.join(
                dim_date,
                df.funded_date_id == dim_date.date_id,
                "left"
            )

            # Remove empty strings from 'fund_description' and set them to NULL.
            df = df.withColumn(
                "source_description",
                F.when(F.trim(df.source_description) == "", None)
                .otherwise(df.source_description)
            )

            # Select the columns for the dimension table and rename for clarity.
            dim_funds = df.select(
                F.col("object_id").alias("nk_fund_id"),
                F.col("name").alias("fund_name"),
                F.col("raised_amount_usd"),
                F.col("funded_date_id").alias("funded_at"),
                F.col("source_description").alias("fund_description")
            )
            
            # Write the transformed data to MinIO
            dim_funds.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(f"{transformed_data_path}/dim_funds")
                
        except Exception as e:
            raise AirflowException(f"Error processing funds table: {str(e)}")
        
        finally:
            if spark:
                spark.stop()
                print("Spark session stopped.")
                

    @staticmethod
    def _investments(incremental, date):
        """
        Extracts and transforms investments table from the staging area to the warehouse.

        Args:
            incremental (bool): Whether to load only today's data.
            date (str): Execution date passed from Airflow (format: YYYY-MM-DD)
        """
        spark = None
        try: 
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform from Staging - Investments") \
                .getOrCreate()
                
            # Define query for extracting data
            query = "(SELECT * FROM staging.investments) AS data"
            if incremental:
                query = f"(SELECT * FROM staging.investments WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 day') AS data"
                
            # Read data from PostgreSQL staging area
            df = spark.read.jdbc(
                url = postgres_warehouse,
                table = query,
                properties = postgres_properties
            ) 
            
            # Check if DataFrame is empty
            if df.isEmpty():
                spark.stop()
                print("No data to process for investments table.")
                return
            
            # Transform data
            
            # Extract dimension tables needed for joins.
            dim_date = extract_warehouse(spark, table_name="dim_date", schema="warehouse")
            dim_company = extract_warehouse(spark, table_name="dim_company", schema="warehouse")
            dim_funds = extract_warehouse(spark, table_name="dim_funds", schema="warehouse")
            stg_funding_rounds = extract_warehouse(spark, table_name="funding_rounds", schema="staging")

            # Join with dim_company to get the company's surrogate key (sk_company_id).
            df = df.join(
                dim_company.select("sk_company_id", "nk_company_id"),
                df.funded_object_id == dim_company.nk_company_id,
                "inner"
            )

            # Join with dim_fund to get the fund's surrogate key (sk_fund_id).
            df = df.join(
                dim_funds.select("sk_fund_id", "nk_fund_id"),
                df.investor_object_id == dim_funds.nk_fund_id,
                "inner"
            )

            # Prepare staging funding rounds data.
            stg_funding_rounds = stg_funding_rounds.withColumn(
                "funded_at",
                F.date_format("funded_at", "yyyyMMdd").cast("integer")
            )

            # Join with dim_date to get date_id.
            stg_funding_rounds = stg_funding_rounds.join(
                dim_date.select("date_id"),
                stg_funding_rounds.funded_at == dim_date.date_id,
                "inner"
            )

            # Join with the funding rounds staging table to get additional information.
            df = df.join(
                stg_funding_rounds.select(
                    "funding_round_id", "funding_round_type", "participants",
                    "raised_amount_usd", "raised_currency_code",
                    "pre_money_valuation_usd", "post_money_valuation_usd",
                    "funded_at"
                ),
                on="funding_round_id",
                how="left"
            )

            # Select the columns for the fact table and rename for clarity.
            fct_investments = df.select(
                F.col("investment_id").alias("dd_investment_id"),
                F.col("sk_company_id"),
                F.col("sk_fund_id"),
                F.col("funded_at"),
                F.col("funding_round_type"),
                F.col("participants").alias("num_of_participants"),
                F.col("raised_amount_usd"),
                F.col("pre_money_valuation_usd"),
                F.col("post_money_valuation_usd"),
            )
            
            print("Final row count fct_investments:", fct_investments.count())
            fct_investments.show(5)
            
            # Write the transformed data to MinIO
            fct_investments.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(f"{transformed_data_path}/fct_investments")
                
        except Exception as e:
            raise AirflowException(f"Error processing investments table: {str(e)}")
        
        finally:
            if spark:
                spark.stop()
                print("Spark session stopped.")
                

    @staticmethod
    def _ipos(incremental, date):
        """
        Extracts and transforms ipos table from the staging area to the warehouse.

        Args:
            incremental (bool): Whether to load only today's data.
            date (str): Execution date passed from Airflow (format: YYYY-MM-DD)
        """
        spark = None
        try: 
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform from Staging - IPOs") \
                .getOrCreate()
                
            # Define query for extracting data
            query = "(SELECT * FROM staging.ipos) AS data"
            if incremental:
                query = f"(SELECT * FROM staging.ipos WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 day') AS data"
                
            # Read data from PostgreSQL staging area
            df = spark.read.jdbc(
                url = postgres_warehouse,
                table = query,
                properties = postgres_properties
            ) 
            
            # Check if DataFrame is empty
            if df.isEmpty():
                spark.stop()
                print("No data to process for ipos table.")
                return
            
            # Transform data

            # Extract dimension tables needed for joins.
            dim_date = extract_warehouse(spark, table_name="dim_date", schema="warehouse")
            dim_company = extract_warehouse(spark, table_name="dim_company", schema="warehouse")

            # Cast 'ipo_id' to integer.
            df = df.withColumn("ipo_id", F.col("ipo_id").cast("integer"))

            # Join with dim_company to get the company's surrogate key (sk_company_id).
            df = df.join(
                dim_company.select("sk_company_id", "nk_company_id"),
                df.object_id == dim_company.nk_company_id,
                "inner"
            )

            # Add a foreign key 'public_date_id' by formatting 'public_at' to match 'date_id' in dim_date.
            df = df.withColumn(
                "public_date_id",
                F.date_format(df.public_at, "yyyyMMdd").cast("integer")
            )

            # Join with dim_date to get date information based on 'public_date_id'.
            df = df.join(
                dim_date,
                df.public_date_id == dim_date.date_id,
                "left"
            )

            # Convert valuation and raised amounts to USD.
            df = df.withColumn("valuation_amount_usd", to_usd(currency_col="valuation_currency_code", amount_col="valuation_amount"))
            df = df.withColumn("raised_amount_usd", to_usd(currency_col="raised_currency_code", amount_col="raised_amount"))

            # Clean and normalize the stock symbol.
            cleaned_stock_symbol = F.trim(F.lower(F.col("stock_symbol")))
            invalid_symbol = cleaned_stock_symbol.rlike(r"^[\W\d_]+$")  # Identify invalid symbols
            cleaned_data = F.when(invalid_symbol, F.lit(None)).otherwise(cleaned_stock_symbol) # Replace invalid with NULL
            df = df.withColumn("stock_symbol", cleaned_data)

            # Remove unused whitespace and convert values to lowercase.
            df = df.withColumn("source_description", F.trim(F.lower(F.col("source_description"))))

            # Select the columns for the fact table and rename for clarity.
            fct_ipos = df.select(
                F.col("ipo_id").alias("dd_ipo_id"),
                F.col("sk_company_id"),
                F.col("valuation_amount_usd"),
                F.col("raised_amount_usd"),
                F.col("public_date_id").alias("public_at"),
                F.col("stock_symbol"),
                F.col("source_description").alias("ipo_description")
            )
            
            # Write the transformed data to MinIO
            fct_ipos.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(f"{transformed_data_path}/fct_ipos")
                
        except Exception as e:
            raise AirflowException(f"Error processing ipos table: {str(e)}")
        
        finally:
            if spark:
                spark.stop()
                print("Spark session stopped.")
                

    @staticmethod
    def _acquisition(incremental, date):
        """
        Extracts and transforms acquisition table from the staging area to the warehouse.

        Args:
            incremental (bool): Whether to load only today's data.
            date (str): Execution date passed from Airflow (format: YYYY-MM-DD)
        """
        spark = None
        try: 
            # Initialize Spark session
            spark = SparkSession.builder \
                .appName("Extract & Transform from Staging - Acquisition") \
                .getOrCreate()
                
            # Define query for extracting data
            query = "(SELECT * FROM staging.acquisition) AS data"
            if incremental:
                query = f"(SELECT * FROM staging.acquisition WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 day') AS data"
                
            # Read data from PostgreSQL staging area
            df = spark.read.jdbc(
                url = postgres_warehouse,
                table = query,
                properties = postgres_properties
            ) 
            
            # Check if DataFrame is empty
            if df.isEmpty():
                spark.stop()
                print("No data to process for acquisition table.")
                return
            
            # Transform data
            
            # Extract dimension tables needed for joins.
            dim_date = extract_warehouse(spark, table_name="dim_date", schema="warehouse")
            dim_company = extract_warehouse(spark, table_name="dim_company", schema="warehouse")

            # Set alias for acquiring and acquired companies from dim_company.
            dim_company_acquiring = dim_company.alias("acq")
            dim_company_acquired = dim_company.alias("acd")

            # Join with dim_company to get surrogate keys for acquiring and acquired companies.
            df = df.join(
                dim_company_acquiring.select(
                    F.col("sk_company_id").alias("sk_acquiring_company_id"),
                    F.col("nk_company_id").alias("nk_acquiring_company_id")
                ),
                df.acquiring_object_id == F.col("nk_acquiring_company_id"),
                "inner"
            )

            df = df.join(
                dim_company_acquired.select(
                    F.col("sk_company_id").alias("sk_acquired_company_id"),
                    F.col("nk_company_id").alias("nk_acquired_company_id")
                ),
                df.acquired_object_id == F.col("nk_acquired_company_id"),
                "inner"
            )

            # Add a foreign key 'acquired_date_id' by formatting 'acquired_at' to match 'date_id' in dim_date.
            df = df.withColumn(
                "acquired_date_id",
                F.date_format(df.acquired_at, "yyyyMMdd").cast("integer")
            )

            # Join with dim_date to get date information based on 'acquired_date_id'.
            df = df.join(
                dim_date,
                df.acquired_date_id == dim_date.date_id,
                "left"
            )

            # Convert acquisition price to USD.
            df = df.withColumn("price_amount_usd", to_usd(currency_col="price_currency_code", amount_col="price_amount"))

            # Clean and normalize term code.
            cleaned_term_code = F.trim(F.lower(F.col("term_code")))
            df = df.withColumn("term_code", F.when(cleaned_term_code == "", F.lit(None)).otherwise(cleaned_term_code))

            # Clean the source description.
            cleaned_description = F.trim(F.lower(F.col("source_description")))
            df = df.withColumn(
                "source_description",
                F.when(cleaned_description == "", F.lit(None))
                .otherwise(cleaned_description)
            )

            # Select the columns for the fact table and rename for clarity.
            fct_acquisition = df.select(
                F.col("acquisition_id").alias("dd_acquisition_id"),
                F.col("sk_acquiring_company_id"),
                F.col("sk_acquired_company_id"),
                F.col("price_amount_usd"),
                F.col("acquired_date_id").alias("acquired_at"),
                F.col("term_code"),
                F.col("source_description").alias("acquisition_description")
            )
            
            # Write the transformed data to MinIO
            fct_acquisition.write \
                .format("parquet") \
                .mode("overwrite") \
                .save(f"{transformed_data_path}/fct_acquisition")
                
        except Exception as e:
            raise AirflowException(f"Error processing acquisition table: {str(e)}")
        
        finally:
            if spark:
                spark.stop()
                print("Spark session stopped.")
                
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: extract_transform.py <function_name> <incremental> <date>")
        sys.exit(-1)

    function_name = sys.argv[1]
    incremental = sys.argv[2].lower() == 'true'
    date = sys.argv[3]

    # Dictionary mapping function names to their corresponding methods
    function_map = {
        "company": ExtractTransform._company,
        "funds": ExtractTransform._funds,
        "investments": ExtractTransform._investments,
        "ipos": ExtractTransform._ipos,
        "acquisition": ExtractTransform._acquisition
    }

    if function_name in function_map:
        function_map[function_name](incremental, date)
    else:
        print(f"Unknown function name: {function_name}")
        sys.exit(-1)