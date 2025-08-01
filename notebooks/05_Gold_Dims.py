#!/usr/bin/env python
# coding: utf-8

# ### Gold Layer - Dimension Tables
# 
# This notebook transforms Silver Layer tables into cleaned and structured Gold Layer **dimension tables** for analytics.
# 
# It handles multiple source tables from `flights.silver`:
# - `silver_pax` → `dim_passengers`
# - `silver_flights` → `dim_flights`
# - `silver_airports` → `dim_airports`
# 
# Techniques used:
# - Parameterized configuration (via code cells)
# - Surrogate key generation
# - Support for backdated refresh and CDC

# In[ ]:


from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable


# - Silver Table Preview (SQL)

# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM flights.silver.silver_flights
LIMIT 10;


# #### Parameters – Passengers Dimension
# 
# These parameters define how the `silver_pax` table will be transformed into the `dim_passengers` dimension in the gold layer.

# In[ ]:


# Source code for Passengers

# Catalog Name
catalog_name = "flights"

# Key Columns
key_cols = "['passenger_id']"
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modified_date"

# Back-dated Refresh
# Back-dated refresh is an extension of incremental refresh, allowing you to specify a date range within the historical data to be refreshed.
backdated_refresh = ""

# Source Object
source_object = "silver_pax"

# Source Schema
source_schema = "silver"

# Target Schema
target_schema = "gold"

# Target Object
target_object = "dim_passengers"

# Surrogate Key
surrogate_key = "DimPassengersKey"


# #### Parameters – Flights Dimension
# 
# These parameters define how the `silver_flights` table will be transformed into the `dim_flights` dimension in the gold layer.

# In[ ]:


# Source code for Flights

# Catalog Name
catalog_name = "flights"

# Key Columns
key_cols = "['flight_id']"
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modified_date"

# Back-dated Refresh
# Back-dated refresh is an extension of incremental refresh, allowing you to specify a date range within the historical data to be refreshed.
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


# #### Parameters – Airports Dimension
# 
# These parameters define how the `silver_airports` table will be transformed into the `dim_airports` dimension in the gold layer.

# In[ ]:


# Source code for Airports

# Catalog Name
catalog_name = "flights"

# Key Columns
key_cols = "['airport_id']"
key_cols_list = eval(key_cols)

# CDC Column
cdc_col = "modified_date"

# Back-dated Refresh
# Back-dated refresh is an extension of incremental refresh, allowing you to specify a date range within the historical data to be refreshed.
backdated_refresh = ""

# Source Object
source_object = "silver_airports"

# Source Schema
source_schema = "silver"

# Target Schema
target_schema = "gold"

# Target Object
target_object = "dim_airports"

# Surrogate Key
surrogate_key = "DimAirportsKey"


# ## Incremental Data Ingestion
# 
# - This block checks whether a backdated refresh is not being used.
# - If that’s the case, and the target table doesn’t exist yet, it attempts to find the most recent timestamp from the source table (based on the CDC column) to support incremental loading.

# In[ ]:


# Testing to see an output

# Only proceed with incremental ingestion logic if no backdated refresh is set
if len(backdated_refresh) == 0:

    # If the target table doesn't exist yet, find the latest timestamp from the source
    if not(spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}")):

        # Query the latest CDC (Change Data Capture) value from the source table
        last_load = spark.sql(
            f"SELECT max({cdc_col}) FROM {catalog_name}.{source_schema}.{source_object}"
        ).collect()[0][0]

# Show the last load timestamp
last_load


# #### Last Load Date Logic – Support for Incremental & Backdated Refresh
# 
# This block determines the last_load timestamp used to filter records during incremental ingestion.
# 
# 
# It supports both:
# 
# - Standard incremental loads (based on the CDC column)
# - Backdated refreshes (e.g. for reprocessing historical ranges)

# In[ ]:


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


# Read Incremental Data From Source Table
# 
# - Using the previously calculated last_load timestamp, this query filters the source table (silver layer) to load only new or updated records based on the CDC (Change Data Capture) column — `modified_date`. This is the core of the incremental ingestion logic.
# - The logic dynamically adapts to the presence (or absence) of the target table.
# - It uses the maximum CDC value from the target to define the “last load”, enabling seamless dynamic incremental loading.

# In[ ]:


# Read only the new or updated records from the Silver layer since the last load
df_src = spark.sql(
    f"SELECT * FROM flights.{source_schema}.{source_object} WHERE {cdc_col} >= '{last_load}'"
)


# In[ ]:


# Display the filtered source data to verify the incremental load (Upsert) is working as expected
df_src.display()


# ## Old vs. New Records
# 
# - This section prepares the target DataFrame (df_target) to compare and merge against the incoming incremental data (df_src).
# - It handles both cases: when the target table already exists, and when it doesn’t (e.g., during the initial load).

# In[ ]:


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


# In[ ]:


# Visualize the current target data to confirm the schema/logic
df_target.display()


# In[ ]:


# Bonus: a standalone test for creating an empty structure with matching schema
spark.sql(f"""
    SELECT '' AS flight_id, '' AS DimFlightsKey,
           CAST('1900-01-01 00:00:00' AS timestamp) AS create_date,
           CAST('1900-01-01 00:00:00' AS timestamp) AS update_date
    FROM flights.silver.silver_flights
    WHERE 1=0
""").display()


# #### Join Source with Target – Identifying Old vs. New Records
# 
# - This section performs a left join between the incoming incremental data (src) and the existing data in the target table (trg) based on the primary key columns.
# - The goal is to classify records as either existing (to be updated) or new (to be inserted).
# - Enriches source records with metadata from the existing target (like surrogate_key, create_date, update_date).
# - Allows detection of whether a record already exists in the target based on whether surrogate_key is NULL.

# In[ ]:


# Dynamically build the join condition using the key columns
join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])


# In[ ]:


# Register temporary views to enable SQL-based join
df_src.createOrReplaceTempView("src")
df_target.createOrReplaceTempView("trg")

# Perform left join: source (left) + target (right)
df_join = spark.sql(f"""
    SELECT src.*,
           trg.{surrogate_key},
           trg.create_date,
           trg.update_date
    FROM src
    LEFT JOIN trg
    ON {join_condition}
""")


# In[ ]:


# Display the join result for verification
df_join.display()


# - Splitting the Data: Old vs. New Records
# 
#     - This is a crucial part of change data capture (CDC) logic in an incremental ETL pipeline.
#     - Helps ensure only changed or new records are written, minimizing load and duplication.
#     - After the join, records are split into two categories:

# In[ ]:


# Existing records (match found via surrogate key) – candidates for update
df_old = df_join.filter(col(f"{surrogate_key}").isNotNull())

# New records (no match found in target) – candidates for insert
df_new = df_join.filter(col(f"{surrogate_key}").isNull())


# In[ ]:


# Display old records for verification
df_old.display()


# ## Enriching DataFrames with Audit Metadata
# 
# - Adds audit metadata to support data lineage and traceability.
# - Ensures surrogate keys are unique and sequential.
# - Prepares both old and new datasets for the upcoming merge/upsert step.

# ### Preparing DF Old

# In[ ]:


# Adds a current timestamp to the update_date column for all records that already exist in the target. This signals that the record was modified.
df_old_enrich = df_old.withColumn("update_date", current_timestamp())


# In[ ]:


df_old_enrich.display()


# ### Preparing DF New

# In[ ]:


df_new.display()


# - This adds three fields to new records:
#     - `surrogate_key`: A unique identifier for each new row.
#     - `create_date`: Timestamp of initial creation.
#     - `update_date`: Timestamp of initial creation (same as create_date on insert).
# 
# - `monotonically_increasing_id()` is used to ensure uniqueness across partitions when generating surrogate keys.

# In[ ]:


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


# In[ ]:


max_surrogate_key


# In[ ]:


df_new_enrich.display()


# In[ ]:


df_old.display()


# ## Unioning Old and New Records
# 
# - Combine the existing enriched data (df_old_enrich) with the newly enriched records (df_new_enrich) using `unionByName` to ensure columns are matched by name (not position).
# - This operation stacks the datasets vertically, preserving schema alignment.

# In[ ]:


df_union = df_old_enrich.unionByName(df_new_enrich)


# In[ ]:


# Display the unified DataFrame for visual inspection
df_union.display()


# ## Upsert
# 
# - Checks if the target Delta table exists before writing.
# - Performs merge (UPSERT) if the table exists, using a surrogate key.
# - Inserts all data if the table doesn’t exist yet.

# In[ ]:


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


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM flights.gold.dim_flights


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM flights.gold.dim_airports


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM flights.gold.dim_passengers


# ## Working with Slowing Changing Dimensions Type 1
# 
# - Type 1 handles changes by **overwriting** existing records without keeping historical data.
# - Useful when **historical accuracy isn't required**, and only the latest values matter.
# - Implemented using **upsert logic** based on surrogate keys and `modified_date`.
# 
# - In this project we have examples for this tables:
#     - `dim_passengers` → `dim_passengers_scd.csv` file
#     - `dim_flights` → `dim_flights_scd.csv` file
#     - `dim_airports` → `dim_airports_scd.csv` file

# #### Example with Passengers SCD
# 
# 1.	Identify Updated Data for SCD Testing
# 
#     - Locate updated test data files for SCDs (e.g. passengers_scd).
#     - Confirm that certain records (like P0049) have modified attributes (e.g., name changed to “Justin Thomas”).
# 
# 2.	Query the Existing Record
# 
#     - Use a query such as SELECT * FROM passengers WHERE passenger_id = 'P0049' to compare the current value before running the pipeline.
# 
#         | Name            | Gender | Nationality |
#         |-----------------|--------|-------------|
#         | Douglas Montoya | Female | Pakistan    |
# 
# 3.	Upload the Modified Data
# 
#     - Upload the updated `dim_passengers_scd.csv` data file into the raw volume.
# 
# 4.	Run the Autoloader Pipeline
#     
#     - Trigger the Autoloader (Bronze) pipeline to incrementally load the new data.
#     - Confirm new rows have been added (e.g., record count increases from 220 to 235).
# 
# 5.	Run the DLT (Silver) Pipeline
#     
#     - Go to Workflows → Pipelines and run the Dimension Transformation (DT) pipeline.
#     - This pipeline performs the upsert based on modified_date.
# 
# 6.	Confirm New and Updated Records
#     
#     - Query the silver-level passengers table: `SELECT * FROM flights.silver.passengers`
#     - Confirm that records are either updated or newly inserted according to the business logic.
# 
# 7.	Run the Dimension Builder
#     
#     - Run the notebook responsible for building dimensions from the silver table.
#     - This will further upsert the data into the gold-level dimensional models.
# 
# 8.	Final Validation
#     
#     - Query the dimension table and confirm that the record (e.g., P0049) reflects the updated value (Justin Thomas).
#     - `SELECT * FROM flights.gold.dim_passengers WHERE passenger_id = 'P0049'`
# 
#         | Name          | Gender | Nationality |
#         |---------------|--------|-------------|
#         | Justin Thomas | Female | Tokelau     |

# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM flights.gold.dim_passengers
WHERE passenger_id = 'P0049'

