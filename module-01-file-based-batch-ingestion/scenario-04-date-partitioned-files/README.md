# Scenario 4: Date-Partitioned File Ingestion

## Objective

Practise reading files from date-partitioned folders for a specific
day and a specific hour using Azure Databricks and PySpark.

## Folder Structure

```text
date_partitioned/
└── year=2026/
    └── month=07/
        ├── day=28/
        │   ├── hour=09/
        │   └── hour=10/
        └── day=29/
            └── hour=09/
