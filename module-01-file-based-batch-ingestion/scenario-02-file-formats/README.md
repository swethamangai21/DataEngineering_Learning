# Scenario 2: Reading Different File Formats

## Objective

Practise reading standard JSON, multiline JSON, nested JSON and Parquet
files using PySpark in Azure Databricks.

## Scenarios Completed

- Read a standard JSON Lines file.
- Read a multiline JSON file using `multiLine=true`.
- Read a nested JSON file.
- Access nested fields using `col()` and dot notation.
- Flatten nested JSON fields using `select()` and `alias()`.
- Read Parquet data and inspect its stored schema.

## File Formats

### Standard JSON

Each line contains one complete JSON object.

```python
spark.read.format("json").load(standard_json_path)
