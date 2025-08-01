#!/usr/bin/env python
# coding: utf-8

# ### Gold Layer - Fact Table
# 
# This notebook transforms Silver Layer tables into a cleaned and structured **Gold Layer fact table** ready for analytics.
# 
# **Key techniques used:**
# - Parameterized configuration (via code cells)
# - Surrogate key generation
# - Support for backdated refresh and Change Data Capture (CDC)

# In[ ]:


from pyspark.sql.functions import *
from delta.tables import DeltaTable


# #### Parameters

# In[ ]:


# Source code for Passengers

# Catalog Name
catalog_name = "flights"

# Source Schema
source_schema = "silver"

# Source Object
source_object = "silver_bookings"

# CDC Column
cdc_col = "modified_date"

# Back-dated Refresh
# Back-dated refresh is an extension of incremental refresh, allowing you to specify a date range within the historical data to be refreshed.
backdated_refresh = ""

# Source Fact Table
fact_table = f"{catalog_name}.{source_schema}.{source_object}"

# Target Schema
target_schema = "gold"

# Target Object
target_object = "fact_bookings"

# Fact Key Columns List
fact_key_cols = ["DimPassengersKey", "DimFlightsKey", "DimAirportsKey", "booking_date"]



# Defines a list of dimension tables with:
# 
# - Fully qualified table names
# - Aliases for join logic
# - Explicit join keys (fact column, dimension column)
# 
# Also defines the columns to retain from the fact table besides the surrogate keys.
# 
# Used for dynamically generating joins and column selections later.

# In[ ]:


dimensions = [
    {
        "table": f"{catalog_name}.{target_schema}.dim_passengers",
        "alias": "dim_passengers",
        "join_keys": [("passenger_id", "passenger_id")] # (fact_col, dim_col)
    },
    {
        "table": f"{catalog_name}.{target_schema}.dim_flights",
        "alias": "dim_flights",
        "join_keys": [("flight_id", "flight_id")] # (fact_col, dim_col)
    },
    {
        "table": f"{catalog_name}.{target_schema}.dim_airports",
        "alias": "dim_airports",
        "join_keys": [("airport_id", "airport_id")] # (fact_col, dim_col)
    }
]

# Columns you want to keep from the Fact table (besides the surrogate keys)
fact_columns = ["amount", "booking_date", "modified_date"]


# In[ ]:


dimensions


# #### Determine Last Load Timestamp
# 
# - Checks if a backdated refresh is requested; otherwise uses CDC for incremental loading.
# - If the fact table exists, retrieves the most recent modified_date.
# - Defaults to a base timestamp (1900-01-01) if no previous data is found.

# In[ ]:


# This is our Incremental Data

# No Back-dated Refresh
if len(backdated_refresh) == 0:

    # If table exists in the destination
    if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):

        last_load = spark.sql(f"SELECT max({cdc_col}) FROM {catalog_name}.{target_schema}.{target_object}").collect()[0][0]

    # Yes back-dated refresh
    else:

        last_load = "1900-01-01 00:00:00"

else:

    last_load = backdated_refresh

# Test the Last Load
last_load


# ## Function to Build Dynamic Fact Query
# 
# - Dynamically constructs a SQL query that joins the fact table to its dimension tables.
# - Adds surrogate key columns: `dim_passengers` → `DimPassengersKey`.
# - Applies incremental filtering using the previously calculated `last_load` timestamp.

# In[ ]:


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


# In[ ]:


query = generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, last_load)


# In[ ]:


print(query)


# #### Data Frame DF_FACT

# In[ ]:


df_fact = spark.sql(query)


# In[ ]:


df_fact.display()


# In[ ]:


df_fact.groupBy("DimPassengersKey", "DimAirportsKey", "DimFlightsKey").count().display()


# In[ ]:


# Duplicated Records
df_fact.groupBy("DimPassengersKey", "DimAirportsKey", "DimFlightsKey").count().filter("count > 1").display()


# In[ ]:


# Duplicated Record Example
df_fact.filter((col('DimPassengersKey') == 180) & (col('DimairportsKey') == 44) & (col('DimFlightsKey') == 8)).display()


# ### Upsert
# 
# - Dynamically builds the merge condition using the list of fact key columns.
# - Checks if the target Delta table exists before writing.
# - Performs merge (UPSERT) using the fact keys and a CDC (change data capture) column to resolve updates.
# - Inserts all data if the table doesn’t exist yet.

# In[ ]:


# Fact Key Columns Merge Condition
fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str


# In[ ]:


# Check if the target Delta table already exists in the metastore
if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):

    # Load the existing Delta table as a DeltaTable object to perform merge operations (UPSERT)
    dlt_obj = DeltaTable.forName(spark, f"{catalog_name}.{target_schema}.{target_object}")

    # Perform the UPSERT using the fact keys as the matching condition
    # - If a match is found and the new change data capture column (cdc_col) is more recent or equal, update all fields.
    # - If no match is found, insert the new record.
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


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM flights.gold.fact_bookings


# In[ ]:


df = spark.sql("SELECT * FROM flights.gold.dim_airports").groupBy("DimAirportsKey").count().filter(col("count") > 1)
df.display()


# In[ ]:


df = spark.sql("SELECT * FROM flights.gold.dim_flights").groupBy("DimFlightsKey").count().filter(col("count") > 1)
df.display()


# In[ ]:


df = spark.sql("SELECT * FROM flights.gold.dim_passengers").groupBy("DimPassengersKey").count().filter(col("count") > 1)
df.display()

