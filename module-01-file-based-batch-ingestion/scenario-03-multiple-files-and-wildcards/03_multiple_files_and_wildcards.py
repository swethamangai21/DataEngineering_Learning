# Databricks notebook source
#Define the schema

from pyspark.sql.types import(
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

multiple_files_path=(
    "/Volumes/de_learning/file_ingestion/source_files/csv/multiple_files/"
)

#Read the entire folder

all_orders_df=(
    spark.read
    .format("csv")
    .option("header","true")
    .option("dateFormat","yyyy-MM-dd")
    .schema(orders_schema)
    .load(multiple_files_path)
)

display(all_orders_df)

# COMMAND ----------

#When Spark reads multiple files from a folder, it combines all their records into one logical DataFrame

# COMMAND ----------

#Read selected files using a wildcard

wildcard_path="/Volumes/de_learning/file_ingestion/source_files/csv/multiple_files/orders_202607*.csv"

wildcard_orders_df=(
    spark.read
    .format("csv")
    .option("header","true")
    .option("dateFormat","yyyy-MM-dd")
    .schema(orders_schema)
    .load(wildcard_path)
)

display(wildcard_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC