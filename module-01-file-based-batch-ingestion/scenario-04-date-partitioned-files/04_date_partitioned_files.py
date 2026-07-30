# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DecimalType
)

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DecimalType(10, 2), True),
    StructField("order_status", StringType(), True)
])

# COMMAND ----------

#Define the paths

base_path=("/Volumes/de_learning/file_ingestion/source_files/date_partitioned/")

day_path=(
    f"{base_path}"
    "year=2026/month=07/day=28"
)

# COMMAND ----------

#Read all files for July 28

day_orders_df=(
    spark.read
    .format("csv")
    .option("header","true")
    .option("dateFormat","yyyy-MM-dd")
    .option("basePath",base_path)
    .schema(orders_schema)
    .load(day_path)
)

display(day_orders_df)

# COMMAND ----------

# basePath-tells Spark where the partition-folder structure begins. Your partition structure starts below: date_partitioned/


# COMMAND ----------

#Read files for one specific hour

hour_path=(
    f"{base_path}"
    "year=2026/month=07/day=28/hour=09/"
)

hour_orders_df=(
    spark.read
    .format("csv")
    .option("header","true")
    .option("dateFormat","yyyy-MM-dd")
    .option("basePath",base_path)
    .schema(orders_schema)
    .load(hour_path)
)

display(hour_orders_df)