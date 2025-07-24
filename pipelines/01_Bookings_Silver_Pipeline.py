import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

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

