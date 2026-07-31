# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists de_learning.bronze;

# COMMAND ----------

# MAGIC %sql 
# MAGIC show schemas in de_learning

# COMMAND ----------

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

initial_load_path="/Volumes/de_learning/file_ingestion/source_files/csv/multiple_files/"

initial_orders_df=(
    spark.read
    .format("csv")
    .option("header","true")
    .option("dateFormat","yyyy-MM-dd")
    .schema(orders_schema)
    .load(initial_load_path)
)

display(initial_orders_df)

# COMMAND ----------

(initial_orders_df.write
.format("delta")
.mode("overwrite")
.saveAsTable("de_learning.bronze.orders_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from de_learning.bronze.orders_bronze;

# COMMAND ----------

#Read the new incoming file

append_file_path="/Volumes/de_learning/file_ingestion/source_files/csv/append_load/orders_20260731.csv"

new_orders_df=(
    spark.read
    .format("csv")
    .option("header","true")
    .option("dateFormat","yyyy-MM-dd")
    .schema(orders_schema)
    .load(append_file_path)
)

display(new_orders_df)

# COMMAND ----------

#Append the new records to the Bronze table

(new_orders_df.write
 .format("delta")
 .mode("append")
 .saveAsTable("de_learning.bronze.orders_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from de_learning.bronze.orders_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC