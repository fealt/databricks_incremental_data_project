#!/usr/bin/env python
# coding: utf-8

# ### Silver Layer Pipeline

# In[ ]:


import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


# #### Define Bronze-to-Silver Table _stage_bookings_
# 
# - This Delta Live Table (DLT) loads streaming data from the bookings folder in the Bronze layer
# - It uses spark.readStream to continuously read Delta files and registers the output as a managed DLT table

# In[ ]:


@dlt.table(
    name = "stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights/bronze/bronze_volume/bookings/data/")
    return df


# ### Apply Transformations to Bookings
# 
# - This DLT view applies transformations to the _stage_bookings_ data, including type casting, adding timestamps, and dropping _rescued_data columns

# In[ ]:


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


# ### Define Data Quality Rules
# 
# - A dictionary _rules_ is defined, which includes constraints for data quality checks (e.g., booking_id and passenger_id must not be NULL)

# In[ ]:


rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}


# ### Create Silver Table with Data Expectations
# 
# - This DLT table _silver_bookings_ reads from the transformed view and enforces all data quality rules via @dlt.expect_all_or_drop
# - Rows that violate rules are dropped

# In[ ]:


@dlt.table(
    name = "silver_bookings"
)

@dlt.expect_all_or_drop(rules)

def silver_bookings():
    df = spark.readStream.table("transformations_bookings")
    return df


# ### Query Silver Layer

# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM flights.silver.silver_airports
LIMIT 10;

