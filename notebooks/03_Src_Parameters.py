#!/usr/bin/env python
# coding: utf-8

# ### Define Source Parameters
# 
# - Create an array of dictionaries called src_array, which lists the different data sources (airports, bookings, customers, flights)
# - This array is used to parameterize data ingestion workflows across multiple source datasets

# In[ ]:


src_array = [
    {"src": "airports"},
    {"src": "bookings"},
    {"src": "customers"},
    {"src": "flights"}
]


# #### Store Task Values in Databricks Jobs
# 
# - Store the src_array as a key-value pair (output_key)
# - These values can be accessed by downstream tasks in a Databricks Job to dynamically control their execution

# In[ ]:


# https://docs.databricks.com/gcp/en/jobs/task-values

dbutils.jobs.taskValues.set(key = "output_key", value = src_array)

