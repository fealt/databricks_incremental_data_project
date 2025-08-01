#!/usr/bin/env python
# coding: utf-8

# ### Create a Delta Volume
# 
# - named rawvolume inside the flights.raw catalog and schema

# In[ ]:


get_ipython().run_line_magic('sql', '')
CREATE VOLUME flights.raw.rawvolume


# ### Create Raw Data Directory

# In[ ]:


dbutils.fs.mkdirs("/Volumes/flights/raw/rawvolume/rawdata")


# ### Create Subdirectories for Raw Data

# In[ ]:


dbutils.fs.mkdirs("/Volumes/flights/raw/rawvolume/rawdata/bookings");
dbutils.fs.mkdirs("/Volumes/flights/raw/rawvolume/rawdata/flights");
dbutils.fs.mkdirs("/Volumes/flights/raw/rawvolume/rawdata/customers");
dbutils.fs.mkdirs("/Volumes/flights/raw/rawvolume/rawdata/airports");


# ### Create Schemas

# In[ ]:


get_ipython().run_line_magic('sql', '')
CREATE SCHEMA flights.bronze;
CREATE SCHEMA flights.silver;
CREATE SCHEMA flights.gold;


# ### Create Raw Volumes for Data Layers
# 
# - create volumes inside the flights.raw schema, preparing storage for bronze, silver, and gold datasets

# In[ ]:


get_ipython().run_line_magic('sql', '')
CREATE VOLUME flights.raw.bronze;
CREATE VOLUME flights.raw.silver;
CREATE VOLUME flights.raw.gold;


# ### Create Volumes for Layered Data

# In[ ]:


get_ipython().run_line_magic('sql', '')
CREATE VOLUME flights.bronze.bronze_volume;
CREATE VOLUME flights.silver.silver_volume;
CREATE VOLUME flights.gold.gold_volume;


# ### Query Data from Bronze Volume
# 
# - Example to query Delta files stored in the bronze volume to verify data availability

# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM delta.`/Volumes/flights/bronze/bronze_volume/flights/data/`

