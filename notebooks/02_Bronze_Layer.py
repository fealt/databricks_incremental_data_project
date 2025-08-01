#!/usr/bin/env python
# coding: utf-8

# ### **INCREMENTAL DATA INGESTION**

# ### Create a Databricks Widget
# 
# - Widgets in Databricks allow parameterization of notebooks, enabling dynamic values at runtime

# In[ ]:


dbutils.widgets.text("src","")


# In[ ]:


# Retrieve Widget Value
# The value can be set at execution time or through the Databricks UI

src_value = dbutils.widgets.get("src")


# ### Load Data with Spark
# 
# - The CloudFiles format automatically detects new files and infers schema, storing checkpoints for incremental loading

# In[ ]:


df = spark.readStream.format("cloudfiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", f"/Volumes/flights/bronze/bronze_volume/{src_value}/checkpoint")\
    .option("cloudFiles.schemaEvolutionMode", "rescue")\
    .load(f"/Volumes/flights/raw/rawvolume/rawdata/{src_value}/")


# ### Write Data to Bronze Layer
# 
# - Writes the ingested raw data into the Bronze layer as Delta table
# - The writeStream function ensures data is appended and checkpointed for streaming pipelines

# In[ ]:


df.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("checkpointLocation", f"/Volumes/flights/bronze/bronze_volume/{src_value}/checkpoint")\
    .option("path", f"/Volumes/flights/bronze/bronze_volume/{src_value}/data")\
    .start()


# #### Query the customers dataset stored in the Bronze volume, verifying that the ingestion pipeline worked

# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM delta.`/Volumes/flights/bronze/bronze_volume/customers/data/`

