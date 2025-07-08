# 🏁 Getting Started: Build an End-to-End ELT Pipeline with Airflow and PySpark

**Welcome to my Learning Logs!** 

This project is part of my hands-on learning journey as I transition into Data Engineering. It demonstrates how to build an ELT pipeline using **Apache Airflow**, **PySpark**, **PostgreSQL**, and **MinIO**.

---

## Project Overview

This project implements an **Extract**, **Load**, and **Transform (ELT) pipeline** using **Apache Airflow** for orchestration and **PySpark** for data processing. The pipeline handles data from **PostgreSQL**, **MinIO**, **and various API sources**.

Key components include:

- **Data Ingestion**

  Extracting data from sources and loading it into MinIO (as a data lake) and a PostgreSQL staging schema.

- **Data Transformation**

  Transforming raw data from staging into a structured warehouse schema.

- **Workflow Orchestration**

  Managing ELT tasks with Apache Airflow, including:

  - **Staging and Warehouse DAGs**: Pipelines for data ingestion and transformation.

  - **Data Profiling DAG**: A separate pipeline for capture data quality.

  - **Dependency Management**: Implementing Airflow features like **_ExternalTaskSensor_** to ensure sequential execution (e.g., warehouse pipeline run only after staging pipeline is complete).

- **Alerting**

  Integrating **Slack alerts via webhooks** for real-time notifications on pipeline/DAG status if there is any fail/error.
    
---

## How the Pipeline Works

![elt-design]()

At its core, the pipeline involves:

Three Primary (structured) Data Sources, scattered in:

- PostgreSQL

- External APIs (JSON)

- MinIO (CSV)

Three Apache Airflow DAGs: Each DAG automates a distinct part of the data workflow:

- `vcapital_staging`

This DAG contains two main tasks:

    - Extract Data: Pulls data from PostgreSQL and external APIs, saving it as CSV files into MinIO. This establishes MinIO as a landing zone for raw ingested data.
    
    - Load Data: Takes the extracted CSVs from MinIO (and other raw data directly from MinIO sources) and loads them into the staging schema within your warehouse database.

- `vcapital_warehouse` 

This DAG manages the transformation of staged data into the final warehouse schema. It includes two sequential tasks:

Process Dimension Tables: This task is dependent on the Staging DAG. It uses an ExternalTaskSensor to ensure that all necessary data has been successfully loaded into the staging schema before it begins. For example, once the company table is loaded into staging, the dim_company transformation task will trigger. This task transforms the staged data, saves the transformed output (as CSV) back to a dedicated transformed-data bucket in MinIO, and then loads it into the warehouse schema.

Process Fact Tables: This task runs only after all dimension tables have been successfully processed and loaded into the warehouse. Similar to dimension processing, it transforms the data, stores the results (as CSV) in the transformed-data MinIO bucket, and finally loads them into the warehouse schema. This sequential order is critical because fact tables look up and rely on the transformed dimension tables for referential integrity and meaningful analysis.



- `data_profiling` 

---

## ⚙️ Requirements

Before getting started, make sure your machine meets the following:

- Memory:
  Minimum 8GB RAM (Recommended: 16GB+, especially for Windows. On Linux, 8GB should be sufficient.)

- Docker (with WSL2 enabled if you're on Windows)

- Python 3.7+ (for generating Fernet key)

- Database Client (DBeaver or any PostgreSQL-compatible SQL client)

---

## 📁 Project Structure

```
elt-airflow-project/
├── dags/
│   ├── vcapital_staging/            # DAG configuration for staging pipeline
│   ├── vcapital_warehouse/          # DAG configuration for warehouse pipeline   
|   ├── data_profiling/              # DAG configuration for data profiling
│   └── helper/                      # Helper functions (callbacks, utils, etc.)
├── dataset/
│   ├── profiling/                   # Profiling database init SQL
│   ├── source/                      # Source database init SQL
│   └── warehouse/                   # Warehouse database init SQL (staging and warehouse schema)
├── include/                         # Airflow connection and variables
├── spark_packages/                  # Spark needs drivers to run PySpark (PostgreSQL JDBC driver, etc.)
├── Dockerfile                       # Custom PySpark and Airflow image
├── docker-compose.yml               # Main Docker Compose config
├── requirements.txt                 # Python packages for Airflow image
├── fernet.py                        # Python script to generate fernet key
└── README.md                        # This guide
```

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone git@github.com:Rico-febrian/elt-pipeline-for-venture-capital-business-with-airflow-pyspark.git
cd elt-pipeline-for-venture-capital-business-with-airflow-pyspark
```

### 2. Generate Fernet Key

This key encrypts credentials in Airflow connections.

```bash
pip install cryptography==45.0.2
python3 fernet.py
```

**Copy the output key** to the `.env` file.

### 3. Create `.env` File for Main Service (Airflow, MinIO and PostgreSQL)

Use the following template and update with your actual configuration:

```ini
# --- Airflow Core Configuration ---
AIRFLOW_UID=50000

# Fernet key for encrypting Airflow connections (generated using fernet.py script)
AIRFLOW_FERNET_KEY=YOUR_GENERATED_FERNET_KEY_HERE

# Airflow metadata database connection URI (eg: postgresql+psycopg2://airflow:airflow@airflow_metadata:5433/airflow)
AIRFLOW_DB_URI=postgresql+psycopg2://<AIRFLOW_DB_USER>:<AIRFLOW_DB_PASSWORD>@<AIRFLOW_METADATA_CONTAINER_NAME>:<AIRFLOW_DB_PORT>/<AIRFLOW_DB_NAME>

# --- Airflow Metadata DB Configuration ---
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow
AIRFLOW_DB_PORT=5433

# --- Source Database Configuration ---
VCAPITAL_DB_USER=postgres
VCAPITAL_DB_PASSWORD=postgres
VCAPITAL_DB_NAME=vcapital_db_src
VCAPITAL_DB_PORT=5435

# --- Data Profiling Database Configuration ---
PROFILING_DB_USER=postgres
PROFILING_DB_PASSWORD=postgres
PROFILING_DB_NAME=data_profiling_db
PROFILING_DB_PORT=5434

# --- Data Warehouse (DWH) Configuration (for staging and warehouse schemas) ---
WAREHOUSE_DB_USER=postgres
WAREHOUSE_DB_PASSWORD=postgres
WAREHOUSE_DB_NAME=warehouse_db
WAREHOUSE_DB_PORT=5436

# --- MinIO Configuration ---
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
MINIO_API_PORT=9000
MINIO_CONS_PORT=9001```

### 4. Setup Airflow Variable and Connections
Airflow needs some variables and connections to run the pipeline properly. You can:

- Set them manually through the Airflow UI

- Or use the preconfigured template:
  - [Airflow variable and connections config]()

For the Slack notifier you can following these steps:
  - **Log in** to your existing Slack account or **create** a new one if you don’t have it yet.
  - **Create a workspace** (if you don’t already have one) and create a dedicated Slack channel where you want to receive alerts.
  - **Create a Slack App**:

    - Go to https://api.slack.com/apps
    - Click **Create New App**
    - Choose **From scratch**
    - Enter your app name and select the workspace you just created or want to use
    - Click **Create App**

  - **Set up an Incoming Webhook** for your app:

    - In the app settings, find and click on **Incoming Webhooks**
    - **Enable Incoming Webhooks** if it’s not already enabled
    - Click **Add New Webhook to Workspace**
    - Select the Slack channel you want alerts to go to and authorize
  
  - **Copy the generated Webhook URL**

    <img src="https://github.com/Rico-febrian/flight-bookings-elt-pipeline-with-airflow/blob/main/pict/slack-webhook.png" alt="webhook-url" width="600"/>


### 5. Build and Start Services

After setting up the `.env` files, Airflow connections, and variables, you can start all the services.

### 6. Open Airflow UI

Access the UI at: [http://localhost:8080](http://localhost:8080) (or your defined port).

Log in with the default credentials:

- Username: `airflow`
- Password: `airflow`
(These are defined in the `airflow-init` service within your `docker-compose.yml`).

---

## ▶️ Run the DAG

- Open the Airflow UI (http://localhost:8080)

- Locate these two DAGs and run the DAG:

  - `vcapital_staging`
  - `vcapital_warehouse`
  - `data_profiling`

---

## DAG Behavior (What to Expect)

This pipeline runs in **two DAGs**, managed by Airflow:

- `vcapital_staging` DAG:


- `vcapital_warehouse` DAG:


- `data_profiling` DAG:


---

## Verify the Results

Since incremental mode and catchup are disabled (set to `False`), the pipeline will runs the **full load** process. So, you can just verify the result by open the database.

- ### Extracted Data in MinIO Bucket
    
    - Log in to the MinIO console (eg. localhost:9000) using the username and password defined in your `.env` file.
    
    - Navigate to the selected bucket.
    
    - You should see the extracted data files in CSV format.

      <img src="https://github.com/Rico-febrian/elt-pipeline-with-dbt-airflow-for-bikes-store-business/blob/main/picts/minio_result.png" alt="minio-result" width="600"/>

- ### Staging and Transformed data in Data Warehouse

  To verify the data in your data warehouse:

  - Open your database client (e.g., DBeaver).

  - Connect to your warehouse database.

  - Look for these schemas:
      
      - bikes_store_staging → contains raw, loaded data
      
      - warehouse → contains clean, transformed tables from DBT

    You can explore the tables, run simple queries, and check the row counts to confirm everything worked as expected.

        
---

## Feedback & Articles

**Thank you for exploring this project!** If you have any feedback, feel free to share, I'm always open to suggestions. Additionally, I write about my learning journey on Medium. You can check out my articles [here](https://medium.com/@ricofebrian731). Let’s also connect on [LinkedIn](https://www.linkedin.com/in/ricofebrian).

---

Happy learning! 🚀