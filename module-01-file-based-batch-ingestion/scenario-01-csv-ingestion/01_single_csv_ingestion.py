# Databricks notebook source
csv_folder_path="/Volumes/de_learning/file_ingestion/source_files/csv/"
files=dbutils.fs.ls(csv_folder_path)
display(files)

# COMMAND ----------

file_path=("/Volumes/de_learning/file_ingestion/source_files/csv/orders_20260729.csv")

orders_df=(
    spark.read
    .format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(file_path)
)

# COMMAND ----------

#format("csv") tells Spark that the source is CSV.

#header = true tells Spark to use the first row as column names.

#inferSchema = true asks Spark to infer column data types instead of reading every column as text.
#  
#Azure Databricks supports reading CSV files from volume paths using the Spark DataFrame reader.

# COMMAND ----------

display(orders_df)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

'''
This shows:
-Column names
-Data types
-Whether each column is nullable
'''

# COMMAND ----------

row_count=orders_df.count()
print(row_count)

# COMMAND ----------

print("Column names:")
print(orders_df.columns)

print("\nNumber of columns:")
print(len(orders_df.columns))

print("\nSample records:")
orders_df.show(5,truncate=False)

# COMMAND ----------

#Read csv without header

file_path=("/Volumes/de_learning/file_ingestion/source_files/csv/orders_without_headers.csv")

orders_no_header_df=(
    spark.read
    .format("csv")
    .option("inferSchema","true")
    .load(file_path))

display(orders_no_header_df)

# COMMAND ----------

column_names=[
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "unit_price",
    "order_status"
]

orders_no_header_df=orders_no_header_df.toDF(*column_names)

display(orders_no_header_df)

# COMMAND ----------

#Read the file with the correct delimiter

pipe_file_path=("/Volumes/de_learning/file_ingestion/source_files/csv/orders_pipe.csv")

orders_pipe_df=(
    spark.read
    .format("csv")
    .option("header","true")
    .option("sep","|")
    .option("inferSchema","true")
    .load(pipe_file_path)
)

display(orders_pipe_df)
                

# COMMAND ----------

#Read the file using an incorrect delimiter

orders_wrong_delimiter_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("sep", ",")
    .option("inferSchema", "true")
    .load(pipe_file_path)
)

display(orders_wrong_delimiter_df)

# COMMAND ----------

#Read the CSV using an explicit schema

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    DecimalType
)

orders_schema=StructType([
    StructField("order_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("product_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("quantity",IntegerType(),True),
    StructField("unit_price",DecimalType(10,2),True),
    StructField("order_status",StringType(),True)
])

orders_explicit_schema_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd")
    .schema(orders_schema)
    .load(
        "/Volumes/de_learning/file_ingestion/source_files/csv/orders_20260729.csv"
    )
)

display(orders_explicit_schema_df)



# COMMAND ----------

orders_explicit_schema_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC