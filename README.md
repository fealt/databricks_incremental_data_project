<img src="https://docs.databricks.com/aws/en/img/logo.svg" height="40">

<br>

## Databricks Incremental Data Injestion Project

### Motivation

By pure chance, I stumbled upon _Ansh Lamba’s_ post on my [LinkedIn](https://www.linkedin.com/in/felipealtermann/) feed and was instantly inspired by his clear, hands-on approach to data engineering, showcased through a project using an aviation dataset. Aviation is a subject I absolutely love and have previously explored in depth during my [Ironhack bootcamp capstone project](https://github.com/fealt/brazilian-regional-airports).
<br>
<br>
The project aligns perfectly with my recent [Databricks Data Analyst Associate](https://credentials.databricks.com/15fed725-6c33-4c2e-9fd7-3f6ee6de619c#acc.qTEn0NwC) and [Databricks Data Engineer Associate](https://credentials.databricks.com/1bcec640-d206-4ee4-af3c-7728d49b05e4#acc.uhCbapWe) certifications, aiming to provide a comprehensive understanding of modern data engineering practices within the Databricks ecosystem.
<br>
<br>
<a href="https://credentials.databricks.com/1bcec640-d206-4ee4-af3c-7728d49b05e4#acc.uhCbapWe" target="_blank">
  <img src="https://www.databricks.com/sites/default/files/2024-05/associate-badge-de.png?v=1717145547" height="200">
</a>
&nbsp;&nbsp;&nbsp;
<a href="https://credentials.databricks.com/15fed725-6c33-4c2e-9fd7-3f6ee6de619c#acc.qTEn0NwC" target="_blank">
  <img src="https://www.databricks.com/sites/default/files/2024-05/associate-badge-da.png?v=1717145244" height="200">
</a>

<br>

## My thanks to

♡ **[Ansh Lamba](https://github.com/anshlambagit)** – for sharing incredible hands-on tutorials and making complex Data Engineering concepts feel simple (and _fun!_)
<br>
♡ **[Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition)** – huge thanks to Databricks for giving _everyone_ a free Lakehouse sandbox to learn and level up in Data & AI
<br>
♡ **[dbdiagram.io](https://dbdiagram.io/home)** – _you guys rock_, not only with your incredible tools but also your dedicated documentation
<br>
♡ **[GitHub](https://github.com/)** – for providing the ultimate platform for collaboration, version control, and open-source learning

<br>

## Tech Stack

- SQL
- Python
- Apache Spark
  - Spark Structured Streaming
- Databricks
  - Auto Loader
  - Lakeflow Declarative Pipelines
  - Unity Catalog Volumes

<br>

## Key Learning Outcomes

- Designing a dimensional data model (star schema)
- Implementing Medallion Architecture
- Incremental data ingestion with Spark Streaming and Databricks Auto Loader
- Parameter parsing and control flow in Databricks jobs
- Building declarative pipelines with Lakeflow
- Creating automated Slowly Changing Dimensions (SCD) builders and fact builders

<br>

## Table of Contents

- [Medallion Architecture Implementation](#medallion-architecture)
- [The Dataset](#dataset)
- [The Setup](#setup)
- [1. From Raw to Bronze Layer](#from-raw-to-bronze)
  - [Incremental Data Ingestion with PySpark Streaming and Auto Loader](#incremental-data-injestion)
  - [Parameter Parsing and Control Flow in Databricks Jobs](#databricks-job)
- [2. From Bronze do Silver Layer](#from-bronze-to-silver)
  - [Lakeflow Declarative Pipelines](#lakeflow-declarative-pipelines)
  - [Orchestrating Databricks Delta Live Tables (DLT) Pipeline for Silver tables](#dlt-pipeline)
- [3. From Silver to Gold Layer](#from-silver-to-gold)
  - [Dimension Tables](#dimension-tables)
    - [Parameters – Flights Dimension](#parameters)
    - [Last Load Date Logic – Support for Incremental & Backdated Refresh](#last-load-logic)
    - [Read Incremental Data From Source Table](#read-incremental-data)
    - [Old vs. New Records](#old-new-records)
    - [Enriching DataFrames with Audit Metadata](#enriching-df)
    - [Upsert](#upsert)
  - [Working with Slowing Changing Dimensions Type 1](#scd-type1)
  - [Fact Table](#fact-table)
    - [Function to Build Dynamic Fact Query](#function-fact-table)
    - [Upsert](#upsert-fact-table)
- [4. Stakeholder's Flight KPI Dashboard](#kpi-dashboard)


<br>
<br>

<a name="medallion-architecture"></a>

## Medallion Architecture Implementation

The project is structured around the Medallion Architecture, which consists of three layers:

### Bronze Layer
- `Data Ingestion:`
  - Incremental data loading using **Spark Structured Streaming** and **Databricks Auto Loader**.
- `Source Data:`
  - Primarily focuses on file-based data sources, reflecting common big data scenarios.
- `Dynamic Solution:`
  - Emphasizes building **dynamic**, deployable notebooks for the bronze layer using **parameter parsing** and **control flow** in Databricks jobs, rather than static solutions.

### Silver Layer
- `Data Transformation:`
  - Utilizes **Databricks Lakeflow Declarative Pipelines** (formerly Delta Live Tables - DLT) for data transformations.
- `Advanced Techniques:`
  - Incorporates **PySpark**, **function calling**, and other newly introduced features within **Databricks Lakeflow** for an enhanced development experience.
- `Dynamic Approach:`
  - Similar to the bronze layer, the silver layer will also be built using a **dynamic approach** for deployable notebooks.

### Gold Layer
- `Core of the Project:`
  - This layer is highlighted as the "heart" of the project due to its innovative approach.
- `Dimensional Data Model:`
  - Building a **star schema**.
- `Automated Builders:`
  - A key innovation is the creation of a **Slowly Changing Dimensions (SCD)** builder and an **automatic fact builder**. These builders will automate the creation and deployment of SCDs and fact tables, making the solution **reusable** across multiple data warehouses. This reflects an industry-level approach to data warehouse development.

<br>

<a name="dataset"></a>

## The Dataset

The source data for the project is indeed made available by Ansh Lamba in [this repository](https://github.com/anshlambagit/AnshLambaYoutube/tree/main/Databricks%20End%20To%20End%20Project) on GitHub.

Based on the defined business problem, the data model design aims to represent the data efficiently for reusability, flexibility and scalability. 

The dataset includes multiple versions and categories of these dimensions to facilitate mastering concepts like Slowly Changing Dimensions (SCD) and incremental data loading.

The dataset is structured around a **central fact table and three dimension tables**, which are foundational to building a **star schema**.

![img](/docs/images/00_dbdiagram.png)

<br>

<a name="setup"></a>

## The Setup

- create your databricks free edition account

- create a volume to store csv files, via ui under catalog or via code, we go for the code option

- create a folder flights under workspaces to store the notebooks

- start a new notebook to create our setup code

<br>

Code example from the [Setup notebook](notebooks/01_Setup.ipynb):

```
# Create Volumes for Layered Data
%sql
CREATE VOLUME flights.bronze.bronze_volume;
CREATE VOLUME flights.silver.silver_volume;
CREATE VOLUME flights.gold.gold_volume;
```

<br>

<a name="from-raw-to-bronze"></a>

## 1. From Raw to Bronze Layer

<a name="incremental-data-injestion"></a>

### Incremental Data Ingestion with Spark Streaming and Auto Loader

**Databricks Auto Loader** _"incrementally and efficiently processes new data files as they arrive in cloud storage. It provides a Structured Streaming source called `cloudFiles`. Given an input directory path on the cloud file storage, the `cloudFiles` source automatically processes new files as they arrive, with the option of also processing existing files in that directory. Auto Loader has support for both Python and SQL in Lakeflow Declarative Pipelines."_ [Source: Databricks](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)

<br>

Code example from the [Bronze Incremental Data Ingestion](notebooks/02_Bronze_Layer.ipynb) notebook:

<br>

- Load Data with Spark

  - The CloudFiles format automatically detects new files and infers schema, storing checkpoints for incremental loading

```
df = spark.readStream.format("cloudfiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", f"/Volumes/flights/bronze/bronze_volume/{src_value}/checkpoint")\
    .option("cloudFiles.schemaEvolutionMode", "rescue")\
    .load(f"/Volumes/flights/raw/rawvolume/rawdata/{src_value}/")
```

<br>

- Write Data to Bronze Layer
  - Writes the ingested raw data into the Bronze layer as Delta table
  - The writeStream function ensures data is appended and checkpointed for streaming pipelines

```
df.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("checkpointLocation", f"/Volumes/flights/bronze/bronze_volume/{src_value}/checkpoint")\
    .option("path", f"/Volumes/flights/bronze/bronze_volume/{src_value}/data")\
    .start()
```

<br>

- Bronze Layer Volumnes `airports`, `flights`, `passengers`, and `bookings`:

![img](/docs/images/01_bronze_layer_tables.png)

<br>

### Idempotency Guaranteed

Another crucial aspect of Databricks Autoloader _"is its ability to provide exact-once processing semantics for ingested files. As new files are discovered in the monitored cloud storage location, their metadata is durably persisted in a cloud-based checkpoint location using a scalable key-value store (Apache RocksDB). (...) This fault tolerance and exactly-once processing semantics are achieved natively by Autoloader, without requiring any manual state management or custom code, simplifying the development and maintenance of reliable and idempotent data ingestion pipelines."_ [Source: Chaos Genius](https://www.chaosgenius.io/blog/databricks-autoloader/)

Here is an example where you can find the `rocksDB` folder in this project:

![img](/docs/images/02_rocksDB.png)

<br>

### Checking the records

Using the [Bronze Incremental Data Ingestion notebook](notebooks/02_Bronze_Layer.ipynb), we _dynamically_ ingest raw source files `dim_airports`, `dim_flights`, `dim_passengers`, and `fact_bookings` from the `/Volumes/flights/raw/rawvolume/rawdata/` hierarchy into the **bronze** layer as Delta data.

**Example:**

In `/Volumes/flights/raw/rawvolume/rawdata/customers/` the [`dim_passengers.csv`](/data) file is sored and contains 200 passenger records.

Because the pipeline uses Auto Loader with checkpointing, rerunning the notebook is idempotent and processes only newly arrived files.

When you add an incremental file (e.g., [`dim_passengers_increment.csv`](/data) with 20 new rows) to the same folder and rerun:

- those rows are appended to the `dim_passengers` bronze table

Repeat the pattern for the other datasets by adding:

- `dim_airports` > upload `dim_airports_increment.csv`
- `dim_flights` > upload `dim_flights_increment.csv`
- `fact_bookings` > upload `fact_bookings_increment.csv`

<br>

<a name="databricks-job"></a>

### Parameter Parsing and Control Flow in Databricks Jobs

The full code is in the [Source Parameters](notebooks/03_Src_Parameters.ipynb) notebook

- Define Source Parameters

  - Create an array of dictionaries called src_array, which lists the different data sources (airports, bookings, customers, flights)
  - This array is used to parameterize data ingestion workflows across multiple source datasets

```
src_array = [
    {"src": "airports"},
    {"src": "bookings"},
    {"src": "customers"},
    {"src": "flights"}
]
```

- Store Task Values in Databricks Jobs

  - Store the src_array as a key-value pair (output_key)
  - These values can be accessed by downstream tasks in a Databricks Job to dynamically control their execution

```
dbutils.jobs.taskValues.set(key = "output_key", value = src_array)
```

- Navigate to the `Jobs and Pipelines` menu to create a new Job:

  - The Python job configuration file is available [at this link](jobs/bronze_ingestion_job.py).

<br>

![img](/docs/images/03_databricks_job_parametrization.png)

<br>

### Job executed successfully

The successful job execution now allows seamless ingestion of new data into the raw volumes, with dynamic and incremental processing

![img](/docs/images/04_databricks_job_parametrization.png)

<br>

<a name="from-bronze-to-silver"></a>

## 2. From Bronze do Silver Layer

<a name="lakeflow-declarative-pipelines"></a>

### Lakeflow Declarative Pipelines

- Silver Layer Pipeline

- Define Bronze-to-Silver Table stage_bookings
  - This Delta Live Table (DLT) loads streaming data from the bookings folder in the Bronze layer
  - It uses spark.readStream to continuously read Delta files and registers the output as a managed DLT table

```
@dlt.table(
    name = "stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights/bronze/bronze_volume/bookings/data/")
    return df
```

- Apply Transformations to Bookings
  - This DLT view applies transformations to the stage_bookings data, including type casting, adding timestamps, and dropping _rescued_data columns

```
@dlt.view(
    name = "transformations_bookings"
)
def transformations_bookings():
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modified_date", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df
```

- Next, navigate to the `Jobs and Pipelines` menu to create a new Pipeline

  - The code was first developed in the [Silver Layer](notebooks/04_Silver_Layer.ipynb) notebook.
  
  - It was then transferred to the [Bookings Silver Transformation](pipelines/01_Bookings_Silver_Pipeline.py) python script for production use.
 
<br>

![img](/docs/images/05_bookings_silver_pipeline.png)

<br>

- And here we have our Bookings Silver tables:

  - `silver_bookings` and `stage_bookings`

<br>

![img](/docs/images/06_silver_tables.png)

<br>

<a name="dlt-pipeline"></a>

### Orchestrating Databricks Delta Live Tables (DLT) Pipeline for Silver tables

- This pipeline reads flight data from a Bronze Delta Lake volume, applies basic transformations, and loads it into a Silver table using Databricks `AUTO CDC`. [Click here to learn more about Databricks AUTO CDC](https://learn.microsoft.com/en-us/azure/databricks/dlt/cdc)

- _transformations_flights_ (DLT View)

  - Reads streaming data from the bronze path.

  - Adds `modified_date` with current timestamp (used for CDC sequencing).

  - Casts `flight_date` to DATE type.

  - Drops the `_rescued_data` column (used for error handling in schema inference).

<br>

```
@dlt.view(
    name = "transformations_flights"
)
def transformations_flights():
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights/bronze/bronze_volume/flights/data/")\
        .withColumn("modified_date", current_timestamp())\
        .withColumn("flight_date", to_date(col("flight_date")))\
        .drop("_rescued_data")
    return df
```

<br>

- _silver_flights_ (Streaming Table)

  - Created using dlt.create_streaming_table.

  - Populated by _create_auto_cdc_flow_ from the _transformations_flights_ view.

  - Uses `flight_id` as the unique key and `modified_date` as the sequencing column.

  - Stores records as **Type 1 Slowly Changing Dimension (SCD)**.

<br>

```
dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "transformations_flights",
    keys = ["flight_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)
```

<br>

- Navigate to the `Jobs and Pipelines` menu to create a new Pipeline.

- In the [Flights Project Pipeline](pipelines/02_Flights_Project_Pipeline.py) file you can find the full python script for production use.

- Let’s explore how a DLT Pipeline dry run looks:

<br>

![img](/docs/images/07_pipeline_dry_run.png)

<br>

### Unified Silver Layer Tables for Business Insights

- Created a unified Silver business view by joining the tables (bookings, pax, flights, airports) using Delta Live Tables.

- Used dlt.readStream() to stream data from silver-layer tables, enabling near real-time insights for business consumption.

- Simplified downstream analytics by consolidating key entities into a single, ready-to-query DLT table (silver_business).

<br>

```
@dlt.table(
    name = "silver_business"
)
def silver_business():
    df = dlt.readStream("silver_bookings")\
        .join(dlt.readStream("silver_pax"), ["passenger_id"])\
        .join(dlt.readStream("silver_flights"), ["flight_id"])\
        .join(dlt.readStream("silver_airports"), ["airport_id"])\
        .drop("modified_date")
    return df
```

<br>

- Monitoring the DLT Pipeline – successfully completed

<br>

![img](/docs/images/08_pipeline_running.png)

<br>

- Materialized View

  - Although not strictly required in the pipeline, the following table was added to demonstrate how to create a materialized view using Delta Live Tables (DLT).

  - This uses dlt.read() instead of dlt.read_stream(), which means it’s treated like a batch (non-streaming) table—similar to a materialized view.

  -	It mirrors the silver_business table for demonstration purposes.

  -	Helpful when showing how DLT handles static (non-streaming) tables vs. streaming inputs.

<br>

```
@dlt.table(
    name = "silver_business_mv"
)
def silver_business_mv():
    df = dlt.read("silver_business")
    return df
```

- New run: monitoring the DLT Pipeline – nearing completion: last step running, others completed

  - After re-running the pipeline, no data was reloaded thanks to Auto Loader’s schema inference and file tracking.

  - Since the files were already processed, Databricks automatically skipped them—ensuring idempotent and efficient execution.

<br>

![img](/docs/images/09_pipeline_running.png)

<br>

<a name="from-silver-to-gold"></a>

## 3. From Silver to Gold Layer

This [Gold Layer Notebook (full code here)](notebooks/05_Gold_Dims.ipynb) transforms Silver Layer tables into cleaned and structured Gold Layer **dimension tables** for analytics.

It handles multiple source tables from `flights.silver`:
- `silver_pax` → `dim_passengers`
- `silver_flights` → `dim_flights`
- `silver_airports` → `dim_airports`

Techniques used:
- Parameterized configuration (via code cells)
- Surrogate key generation
- Support for backdated refresh and CDC

<a name="dimension-tables"></a>

## Dimension Tables

<a name="parameters"></a>

### Parameters – Flights Dimension

These parameters define how the `silver_flights` table will be transformed into the `dim_flights` dimension in the gold layer:

<br>

```
# Catalog Name
catalog_name = "flights"

# Key Columns
key_cols = "['flight_id']"
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modified_date"

# Back-dated Refresh
# Back-dated refresh is an extension of incremental refresh, allowing you to specify a date range
# within the historical data to be refreshed.
backdated_refresh = ""

# Source Object
source_object = "silver_flights"

# Source Schema
source_schema = "silver"

# Target Schema
target_schema = "gold"

# Target Object
target_object = "dim_flights"

# Surrogate Key
surrogate_key = "DimFlightsKey"
```

<br>

<a name="last-load-logic"></a>

### Last Load Date Logic – Support for Incremental & Backdated Refresh

This block determines the last_load timestamp used to filter records during incremental ingestion.

It supports both:

- Standard incremental loads (based on the CDC column)
- Backdated refreshes (e.g. for reprocessing historical ranges)

<br>

```
# Determine the last load timestamp for incremental processing

# Case 1: No backdated refresh specified
if len(backdated_refresh) == 0:

    # If the target table exists, get the max value from the CDC column
    if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):
        last_load = spark.sql(
            f"SELECT max({cdc_col}) FROM {catalog_name}.{target_schema}.{target_object}"
        ).collect()[0][0]

    # If the target table doesn't exist, fall back to a default starting point
    else:
        last_load = "1900-01-01 00:00:00"

# Case 2: Backdated refresh is provided
else:
    last_load = backdated_refresh

# Output the resolved timestamp (for debug/logging)
last_load
```

<br>

<a name="read-incremental-data"></a>

### Read Incremental Data From Source Table

- Using the previously calculated last_load timestamp, this query filters the source table (silver layer) to load only new or updated records based on the CDC (Change Data Capture) column — `modified_date`. This is the core of the incremental ingestion logic.
- The logic dynamically adapts to the presence (or absence) of the target table.
- It uses the maximum CDC value from the target to define the “last load”, enabling seamless dynamic incremental loading.

<br>

```
# Read only the new or updated records from the Silver layer since the last load
df_src = spark.sql(
    f"SELECT * FROM flights.{source_schema}.{source_object} WHERE {cdc_col} >= '{last_load}'"
)
```
<br>

<a name="old-new-records"></a>

### Old vs. New Records

- This section prepares the target DataFrame (df_target) to compare and merge against the incoming incremental data (df_src).
- It handles both cases: when the target table already exists, and when it doesn’t (e.g., during the initial load).

<br>

```
if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):
    # Prepare key columns to fetch existing records from the target table
    key_cols_str_incremental = ", ".join(key_cols_list)

    df_target = spark.sql(f"""
        SELECT {key_cols_str_incremental}, {surrogate_key}, create_date, update_date
        FROM {catalog_name}.{target_schema}.{target_object}
    """)

else:
    # Prepare a dummy empty structure to allow merge logic even if the table doesn't exist yet
    key_cols_str_initial = [f"'' AS {i}" for i in key_cols_list]
    key_cols_str_initial = ", ".join(key_cols_str_initial)

    df_target = spark.sql(f"""
        SELECT {key_cols_str_initial},
               CAST('0' AS INT) AS {surrogate_key},
               CAST('1900-01-01 00:00:00' AS timestamp) AS create_date,
               CAST('1900-01-01 00:00:00' AS timestamp) AS update_date
        WHERE 1=0
    """)
```

<br>

<a name="enriching-df"></a>

### Enriching DataFrames with Audit Metadata

- Adds audit metadata to support data lineage and traceability.
- Ensures surrogate keys are unique and sequential.
- Prepares both old and new datasets for the upcoming merge/upsert step.

```
# Adds a current timestamp to the update_date column for all records that already exist in the target. This signals that the record was modified.
df_old_enrich = df_old.withColumn("update_date", current_timestamp())
```

<br>

- This adds three fields to new records:
    - `surrogate_key`: A unique identifier for each new row.
    - `create_date`: Timestamp of initial creation.
    - `update_date`: Timestamp of initial creation (same as create_date on insert).
- `monotonically_increasing_id()` is used to ensure uniqueness across partitions when generating surrogate keys.

```
# Check if the target table already exists in the catalog
if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):

    # Get the current maximum surrogate key from the target table
    max_surrogate_key = spark.sql(f"""
        SELECT MAX({surrogate_key})
        FROM {catalog_name}.{target_schema}.{target_object}
    """).collect()[0][0]

    # Enrich the new records with:
    # - A new surrogate key (offset from max + unique ID)
    # - Current timestamps for create_date and update_date
    df_new_enrich = df_new.withColumn(f"{surrogate_key}",
                                      lit(max_surrogate_key) + lit(1) + monotonically_increasing_id())\
                          .withColumn("create_date", current_timestamp())\
                          .withColumn("update_date", current_timestamp())

else:
    # If the table does not exist, start surrogate keys from 1
    max_surrogate_key = 0

    # Enrich new records with surrogate key starting at 1,
    # and set current timestamps for create_date and update_date
    df_new_enrich = df_new.withColumn(f"{surrogate_key}",
                                      lit(max_surrogate_key) + lit(1) + monotonically_increasing_id())\
                          .withColumn("create_date", current_timestamp())\
                          .withColumn("update_date", current_timestamp())
```

<br>

<a name="upsert"></a>

### Upsert

- Checks if the target Delta table exists before writing.
- Performs merge (UPSERT) if the table exists, using a surrogate key.
- Inserts all data if the table doesn’t exist yet.

<br>

```
# Check if the target Delta table already exists in the metastore
if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):

    # Load the existing Delta table as a DeltaTable object to perform merge operations (UPSERT)
    dlt_obj = DeltaTable.forName(spark, f"{catalog_name}.{target_schema}.{target_object}")

    # Perform the UPSERT using the surrogate key as the matching condition
    # - If a match is found and the new change data capture column (cdc_col) is more recent or equal, update all fields.
    # - If no match is found, insert the new record.
    dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
                        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()

else:
    # If the table does not exist, create it and load the data using 'append' mode
    # This initializes the Delta table with the unioned dataset
    df_union.write.format("delta")\
                  .mode("append")\
                  .saveAsTable(f"{catalog_name}.{target_schema}.{target_object}")
```

<br>

<a name="scd-type1"></a>

## Working with Slowing Changing Dimensions Type 1

- Type 1 handles changes by **overwriting** existing records without keeping historical data.
- Useful when **historical accuracy isn't required**, and only the latest values matter.
- Implemented using **upsert logic** based on surrogate keys and `modified_date`.

- In this project we have examples for this tables:
    - `dim_passengers` → `dim_passengers_scd.csv` file
    - `dim_flights` → `dim_flights_scd.csv` file
    - `dim_airports` → `dim_airports_scd.csv` file

<br>

#### Example with Passengers SCD

1. Identify Updated Data for SCD Testing

    - Locate updated test data files for SCDs (e.g. passengers_scd).
    - Confirm that certain records (like P0049) have modified attributes (e.g., name changed to “Justin Thomas”).

2. Query the Existing Record

    - Use a query such as SELECT * FROM passengers WHERE passenger_id = 'P0049' to compare the current value before running the pipeline.

```
    | Name            | Gender | Nationality |
    |-----------------|--------|-------------|
    | Douglas Montoya | Female | Pakistan    |
```

3. Upload the Modified Data

    - Upload the updated `dim_passengers_scd.csv` data file into the raw volume.

4. Run the Autoloader Pipeline
    
    - Trigger the Autoloader (Bronze) pipeline to incrementally load the new data.
    - Confirm new rows have been added (e.g., record count increases from 220 to 235).

5. Run the DLT (Silver) Pipeline
    
    - Go to Workflows → Pipelines and run the Dimension Transformation (DT) pipeline.
    - This pipeline performs the upsert based on modified_date.

6. Confirm New and Updated Records
    
    - Query the silver-level passengers table: `SELECT * FROM flights.silver.passengers`
    - Confirm that records are either updated or newly inserted according to the business logic.

7. Run the Dimension Builder
    
    - Run the notebook responsible for building dimensions from the silver table.
    - This will further upsert the data into the gold-level dimensional models.

8. Final Validation
    
    - Query the dimension table and confirm that the record (e.g., P0049) reflects the updated value (Justin Thomas).
    - `SELECT * FROM flights.gold.dim_passengers WHERE passenger_id = 'P0049'`

```
    | Name          | Gender | Nationality |
    |---------------|--------|-------------|
    | Justin Thomas | Female | Tokelau     |
```

<br>

![img](/docs/images/10_scd_gold_dim_airports.png)

<br>

<a name="fact-table"></a>

## Fact Table

This [Gold Layer Notebook (full code here)](notebooks/06_Gold_Fact.ipynb) transforms Silver Layer tables into a cleaned and structured **Gold Layer fact table** ready for analytics.

Key techniques used:
- Parameterized configuration (via code cells)
- Surrogate key generation
- Support for backdated refresh and Change Data Capture (CDC)

<a name="function-fact-table"></a>

### Function to Build Dynamic Fact Query

- Dynamically constructs a SQL query that joins the fact table to its dimension tables.
- Adds surrogate key columns: `dim_passengers` → `DimPassengersKey`.
- Applies incremental filtering using the previously calculated `last_load` timestamp.

<br>

```
def generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, processing_date):

    fact_alias = "f"

    # Base columns to select
    select_columns = [f"{fact_alias}.{col}" for col in fact_columns]

    # Build joins dynamically
    join_clauses = []
    for dim in dimensions:
        table_full = dim["table"]
        alias = dim["alias"]
        table_name = table_full.split('.')[-1]

        # Convert table name to PascalCase
        pascal_name = ''.join(word.capitalize() for word in table_name.split('_'))
        surrogate_key = f"{alias}.{pascal_name}Key"
        select_columns.append(surrogate_key)

        # Build ON clause
        on_conditions = [
            f"{fact_alias}.{fk} = {alias}.{dk}" for fk, dk in dim["join_keys"]
        ]
        join_clause = f"LEFT JOIN {table_full} {alias} ON " + " AND ".join(on_conditions)
        join_clauses.append(join_clause)
        
    # Final SELECT and JOIN clauses
    select_clause = ",\n    ".join(select_columns)
    joins = "\n".join(join_clauses)

    # WHERE clause for incremental filtering
    where_clause = f"{fact_alias}.{cdc_col} >= DATE('{last_load}')"

    # Final query
    query = f"""
SELECT
    {select_clause}
FROM
    {fact_table} {fact_alias}
{joins}
WHERE
    {where_clause}
    """.strip()

    return query
```

<br>

<a name="upsert-fact-table"></a>

### Upsert

- Dynamically builds the merge condition using the list of fact key columns.
- Checks if the target Delta table exists before writing.
- Performs merge (UPSERT) using the fact keys and a CDC (change data capture) column to resolve updates.
- Inserts all data if the table doesn’t exist yet.

<br>

```
# Fact Key Columns Merge Condition
fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])

# Check if the target Delta table already exists in the metastore
if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):

    # Load the existing Delta table as a DeltaTable object to perform merge operations (UPSERT)
    dlt_obj = DeltaTable.forName(spark, f"{catalog_name}.{target_schema}.{target_object}")

    # Perform the UPSERT using the fact keys as the matching condition
    # If a match is found and the new change data capture column (cdc_col) is more recent or equal,
    # update all fields.
    # If no match is found, insert the new record.
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()

else:
    # If the table does not exist, create it and load the data using 'append' mode
    # This initializes the Delta table with the fact dataset
    df_fact.write.format("delta")\
                 .mode("append")\
                 .saveAsTable(f"{catalog_name}.{target_schema}.{target_object}")
```

<br>

![img](/docs/images/11_fact_gold.png)

<br>

<a name="kpi-dashboard"></a>

## Stakeholder's Flight KPI Dashboard

As an additional step beyond the video course, this dashboard was developed to showcase the insights derived from the curated Gold layer.

This dashboard provides a consolidated view of key performance indicators (KPIs) for the _hypothetical Travel Agency’s_ **sales and operations** during the selected date range.

It supports decision-making by offering insights into sales trends, airline performance, passenger distribution, and top destinations.

<br>

![img](/docs/images/12_kpi_dashboard.png)

### Date Range Filter

- Functionality: Enables dynamic filtering of all metrics in the dashboard based on a custom time window.
- Current Range Displayed: July 1 to July 21, 2025

### Section 1: Travel Agency (Header Panel)

- Decorative section for branding and visual identification.
- Highlights the Travel Agency domain.

### Section 2: Daily Sales Report (Bar Chart – Left)

- Purpose: Displays total daily sales across different city locations.
- Insight:
  - Berryport is the top-performing location, followed by Bennettcide and Cooperview.
  - Sales figures decrease progressively across the ranked locations.
- Metric Unit: Approximate revenue in units (currency or ticket value, TBD).

### Section 3: Total Bookings by Airline (Bar Chart – Right)

- Purpose: Shows number of flight bookings per airline.
- Additional Insight:
  - A “Profit Line” is visually represented to indicate performance targets or thresholds.

### Section 4: Sales Range Filter (Top Right Panel)

- Purpose: Interactive slider to filter airlines by a specified sales range.
- Current Range: 309.36 to 1448.27 (assumed unit: currency or average ticket value)

### Section 5: Passenger Nationality Distribution (Map – Bottom)

- Purpose: Visual map highlighting the origin countries of passengers.
- Regions Highlighted: Includes countries from Africa, Latin America, Asia, and Oceania.
- Tool: Mapbox-powered interactive layer for regional insights.

### Section 6: Top Destinations (List – Bottom Left)

- Purpose: Lists most visited cities based on booking data.
- Interaction: Scrollable for exploring all destinations.

<br>

## The end

Thank you!

<br>

<br>
