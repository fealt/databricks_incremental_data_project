import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#
# Bookings Data
#

@dlt.table(
    name = "stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights/bronze/bronze_volume/bookings/data/")
    return df

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

rules = {
    "rule1": "booking_id IS NOT NULL",
    "rule2": "passenger_id IS NOT NULL"
}

@dlt.table(
    name = "silver_bookings"
)

@dlt.expect_all_or_drop(rules)

def silver_bookings():
    df = spark.readStream.table("transformations_bookings")
    return df


#
# Flights Data
#

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

# source info and code
# https://learn.microsoft.com/en-us/azure/databricks/dlt/cdc

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "transformations_flights",
    keys = ["flight_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)


#
# Passengers Data
#

@dlt.view(
    name = "transformations_pax"
)
def transformations_pax():
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights/bronze/bronze_volume/customers/data/")\
        .withColumn("modified_date", current_timestamp())\
        .drop("_rescued_data")
    return df


dlt.create_streaming_table("silver_pax")

dlt.create_auto_cdc_flow(
    target = "silver_pax",
    source = "transformations_pax",
    keys = ["passenger_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)


#
# Airports Data
#

@dlt.view(
    name = "transformations_airports"
)
def transformations_airports():
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights/bronze/bronze_volume/airports/data/")\
        .withColumn("modified_date", current_timestamp())\
        .drop("_rescued_data")
    return df


dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "transformations_airports",
    keys = ["airport_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)


#
# Silver Business View
#

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


#
# Materialized View
#

@dlt.table(
    name = "silver_business_mv"
)
def silver_business_mv():
    df = dlt.read("silver_business")
    return df


