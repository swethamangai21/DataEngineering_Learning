# Scenario 5: Bronze Full and Append-Only Loads

## Objective

Practise performing an initial full load and a subsequent append-only
load into a Bronze Delta table using Azure Databricks and Unity Catalog.

## Architecture

```text
ADLS landing CSV files
        ↓
Spark DataFrame
        ↓
Unity Catalog managed Bronze Delta table
