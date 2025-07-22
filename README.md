<img src="https://docs.databricks.com/aws/en/img/logo.svg" height="40">

<br>

## Databricks Incremental Data Injestion Project

### Motivation

By pure chance, I stumbled upon Ansh Lamba’s post on my [LinkedIn](https://www.linkedin.com/in/felipealtermann/) feed and was instantly inspired by his clear, hands-on approach to data engineering, showcased through a project using an aviation dataset. Aviation is a subject I absolutely love and have previously explored in depth during my [Ironhack bootcamp capstone project](https://github.com/fealt/brazilian-regional-airports).
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

### My love and thanks to:

♡ **[Ansh Lamba](https://github.com/anshlambagit)** – for sharing incredible hands-on tutorials and making complex Data Engineering concepts feel simple (and _fun!_)
<br>
♡ **[Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition)** – huge thanks to Databricks for giving _everyone_ a free Lakehouse sandbox to learn and level up in Data & AI
<br>
♡ **[dbdiagram.io](https://dbdiagram.io/home)** – _you guys rock_, not only with your incredible tools but also your dedicated documentation
<br>
♡ **[GitHub](https://github.com/)** – for providing the ultimate platform for collaboration, version control, and open-source learning

<br>

Stack:
- SQL
- Python
- Apache Spark
  - Spark Structured Streaming
- Databricks
  - Auto Loader
  - Lakeflow Declarative Pipelines
  - Unity Catalog volumes

<br>

Key Learning Outcomes:
- Mastering Databricks
- Implementing Medallion Architecture
- Incremental data ingestion with Spark Streaming and Databricks Auto Loader
- Parameter parsing and control flow in Databricks jobs
- Building declarative pipelines with Lakeflow
- Designing and implementing a dimensional data model (star schema)
- Creating automated Slowly Changing Dimensions (SCD) builders and fact builders

<br>

- [Medallion Architecture Implementation](#medallion-architecture)
- [Dataset](#dataset)
- [Setup](#setup)
- [Incremental Data Ingestion with PySpark Streaming and Auto Loader](#incremental-data-injestion)
- [Parameter Parsing and Control Flow in Databricks Jobs](#databricks-job)
- [Lakeflow Declarative Pipelines](#lakeflow-declarative-pipelines)
- [Orchestrating Databricks Delta Live Tables Pipelines](#dlt-pipeline)

<br>

<a name="medallion-architecture"></a>

## Medallion Architecture Implementation

The project is structured around the Medallion Architecture, which consists of three layers:

### Bronze Layer
- `Data Ingestion:` Incremental data loading using **Spark Structured Streaming** and **Databricks Auto Loader**.
- `Source Data:` Primarily focuses on file-based data sources, reflecting common big data scenarios.
- `Dynamic Solution:` Emphasizes building **dynamic**, deployable notebooks for the bronze layer using **parameter parsing** and **control flow** in Databricks jobs, rather than static solutions.

### Silver Layer
- `Data Transformation:` Utilizes **Databricks Lakeflow Declarative Pipelines** (formerly Delta Live Tables - DLT) for data transformations.
- `Advanced Techniques:` Incorporates **PySpark**, **function calling**, and other newly introduced features within **Databricks Lakeflow** for an enhanced development experience.
- `Dynamic Approach:` Similar to the bronze layer, the silver layer will also be built using a **dynamic approach** for deployable notebooks.

### Gold Layer
- `Core of the Project:` This layer is highlighted as the "heart" of the project due to its innovative approach.
- `Dimensional Data Model:` Building a **star schema**.
- `Automated Builders:` A key innovation is the creation of a **Slowly Changing Dimensions (SCD)** builder and an **automatic fact builder**. These builders will automate the creation and deployment of SCDs and fact tables, making the solution **reusable** across multiple data warehouses. This reflects an industry-level approach to data warehouse development.

<br>

<a name="dataset"></a>

## Dataset

The source data for the project is indeed made available by Ansh Lamba in [this repository](https://github.com/anshlambagit/AnshLambaYoutube/tree/main/Databricks%20End%20To%20End%20Project) on GitHub.

Based on the defined business problem, the data model design aims to represent the data efficiently for reusability, flexibility and scalability. 

The dataset includes multiple versions and categories of these dimensions to facilitate mastering concepts like Slowly Changing Dimensions (SCD) and incremental data loading.

The dataset is structured around a **central fact table and three dimension tables**, which are foundational to building a **star schema**.

![img](/docs/images/dbdiagram.png)

<br>

<a name="setup"></a>

## Setup

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

<a name="incremental-data-injestion"></a>

## Incremental Data Ingestion with Spark Streaming and Auto Loader

**Databricks Auto Loader** _"incrementally and efficiently processes new data files as they arrive in cloud storage. It provides a Structured Streaming source called `cloudFiles`. Given an input directory path on the cloud file storage, the `cloudFiles` source automatically processes new files as they arrive, with the option of also processing existing files in that directory. Auto Loader has support for both Python and SQL in Lakeflow Declarative Pipelines."_ [Source: Databricks](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)

Code example from the [Bronze Incremental Data Ingestion notebook](notebooks/02_Bronze_Layer.ipynb):

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

### Idempotency Guaranteed

Another crucial aspect of Databricks Autoloader _"is its ability to provide exact-once processing semantics for ingested files. As new files are discovered in the monitored cloud storage location, their metadata is durably persisted in a cloud-based checkpoint location using a scalable key-value store (Apache RocksDB). (...) This fault tolerance and exactly-once processing semantics are achieved natively by Autoloader, without requiring any manual state management or custom code, simplifying the development and maintenance of reliable and idempotent data ingestion pipelines."_ [Source: Chaos Genius](https://www.chaosgenius.io/blog/databricks-autoloader/)

Here is an example where you can find the `rocksDB` folder in this project:

![img](/docs/images/01_rocksDB.png)

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

## Parameter Parsing and Control Flow in Databricks Jobs

The full code is in the [Source Parameters](notebooks/03_Src_Parameters.ipynb) notebook.

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

- Next, navigate to the `Jobs and Pipelines` menu to create a new Job:

The Python job configuration file is available [at this link](jobs/bronze_ingestion_job.py).

<br>

![img](/docs/images/02_databricks_job_parametrization.png)

<br>

![img](/docs/images/03_databricks_job_parametrization.png)

<br>

### The successful job execution now allows seamless ingestion of new data into the raw volumes, with dynamic and incremental processing:

![img](/docs/images/04_databricks_job_parametrization.png)

<br>

<a name="lakeflow-declarative-pipelines"></a>

## Lakeflow Declarative Pipelines

- Silver Layer Pipeline

The full code is in the [Silver Layer](notebooks/04_Silver_Layer.ipynb) notebook.

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

<br>

<a name="dlt-pipeline"></a>

## Orchestrating Databricks Delta Live Tables Pipelines

- Next, navigate to the `Jobs and Pipelines` menu to create a new Pipeline:




















