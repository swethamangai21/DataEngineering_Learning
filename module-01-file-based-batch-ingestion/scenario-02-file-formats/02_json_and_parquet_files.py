# Databricks notebook source
#Read a standard JSON file

standard_json_path=("/Volumes/de_learning/file_ingestion/source_files/json/orders_standard.json")

orders_json_df=(
    spark.read
    .format("json")
    .load(standard_json_path)
)

display(orders_json_df)

# COMMAND ----------

#Unlike CSV, standard JSON already contains field names such as order_id and customer_id, so options such as header and sep are not required.

# COMMAND ----------

#Read a Multiline JSON File

multiline_json_path=("/Volumes/de_learning/file_ingestion/source_files/json/orders_multiline.json")

orders_multiline_df=(
    spark.read
    .format("json")
    .option("multiline","true")
    .load(multiline_json_path)
)

display(orders_multiline_df)

# COMMAND ----------

#Read the nested JSON

nested_json_path=(
    "/Volumes/de_learning/file_ingestion/source_files/json/orders_nested.json"
)

orders_nested_df=(
    spark.read
    .format("json")
    .load(nested_json_path)
)

display(orders_nested_df)

# COMMAND ----------

#Inspect the nested schema
orders_nested_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

orders_nested_selected_df = orders_nested_df.select(
    col("order_id"),
    col("order_date"),
    col("customer.customer_id").alias("customer_id"),
    col("customer.customer_name").alias("customer_name"),
    col("customer.city").alias("customer_city"),
    col("product.product_id").alias("product_id"),
    col("product.product_name").alias("product_name"),
    col("product.category").alias("product_category"),
    col("quantity"),
    col("unit_price"),
    col("order_status")
)

display(orders_nested_selected_df)

# COMMAND ----------

# DBTITLE 1,Cell 7
#Read and Write Parquet Files

parquet_output_path=("/Volumes/de_learning/file_ingestion/source_files/parquet/orders_paruet")

(orders_json_df.write
.format("parquet")
.mode("overwrite")
.save(parquet_output_path))


# COMMAND ----------

#Read the Parquet data

orders_parquet_df=(
    spark.read
    .format("parquet")
    .load(parquet_output_path)
)

display(orders_parquet_df)