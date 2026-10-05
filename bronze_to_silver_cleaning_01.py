#!/usr/bin/env python
# coding: utf-8

# ## bronze_to_silver_cleaning_01
# 
# null

# In[1]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# 1. Load Raw Bronze Parquet from OneLake Files
bronze_file_path = "Files/bronze/yellow_taxi/yellow_tripdata_2024-01.parquet"
raw_df = spark.read.parquet(bronze_file_path)

print(f"Raw Bronze Count: {raw_df.count():,}")

# 2. Data Cleansing & Validation Rules:
# - Filter out trips with non-positive distance and fare
# - Eliminate invalid/negative passenger counts
# - Filter illogical dates (must be within January 2024)
# - Calculate derived metric: duration in minutes
silver_df = (
    raw_df
    .filter(
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("passenger_count") > 0) &
        (F.col("tpep_pickup_datetime") >= "2024-01-01 00:00:00") &
        (F.col("tpep_dropoff_datetime") < "2024-02-01 00:00:00") &
        (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    )
    .withColumn(
        "trip_duration_minutes",
        F.round((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60, 2)
    )
    .withColumn("ingestion_timestamp", F.current_timestamp())
)

print(f"Cleaned Silver Count: {silver_df.count():,}")

# 3. Write directly into Lakehouse Managed Delta Table
silver_table_name = "silver_taxi_trips"
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(silver_table_name)
)

print(f"Successfully written to Delta table: {silver_table_name}")


# In[2]:


from pyspark.sql import functions as F

# 1. Read clean data from Silver Delta table
silver_df = spark.read.table("silver_taxi_trips")

# 2. Daily Summary Metric Table (Fact/Aggregate)
gold_daily_metrics = (
    silver_df
    .groupBy(F.to_date("tpep_pickup_datetime").alias("trip_date"))
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.avg("tip_amount"), 2).alias("avg_tip_amount")
    )
    .orderBy("trip_date")
)

# 3. Save as Gold Managed Delta Table
gold_daily_metrics.write.format("delta").mode("overwrite").saveAsTable("gold_daily_taxi_metrics")

# 4. Preview Aggregates
display(gold_daily_metrics.limit(10))


# In[ ]:




