# NHTSA FARS ELT Dashboard

This project delivers a cloud-based ELT pipeline and analytics stack for the National Highway Traffic Safety Administration's (NHTSA) Fatality Analysis Reporting System (FARS) data.

It ingests FARS source files, standardizes and cleans them with Python, lands curated CSV outputs in Azure Blob Storage, loads them into Azure SQL Database through Azure Data Factory, models them with dbt, and prepares the data for Power BI reporting.

---

## Project Overview

- Ingests updated FARS data
- Stores raw and cleaned datasets in Azure Blob Storage
- Uses Python for extraction, standardization, and cleaning
- Uses Azure Data Factory to load cleaned files into Azure SQL staging tables and merge them into production tables
- Uses dbt to build staging, intermediate, fact, and mart models in Azure SQL
- Uses Power BI as the reporting layer on top of the modeled warehouse

---

## ELT Workflow

### 1. **Extract & Load**

- Data is sourced from NHTSA’s public FARS CSV datasets.
- The Python pipeline:
  - Downloads the annual FARS ZIP files
  - Extracts `accident.csv` and `vehicle.csv`
  - Uploads raw source files to the `raw-data` Azure Blob container
  - Standardizes and cleans the files
  - Uploads cleaned outputs to the `processed-data/cleaned/` Blob path
  - Publishes an ADF handoff manifest for downstream loading

### 2. **Transform**

- **Azure Data Factory** performs the loading step by:
  - Clearing Azure SQL staging tables
  - Copying cleaned Blob files into `dbo.accident_stage` and `dbo.vehicle_stage`
  - Running stored procedures to merge staged rows into `dbo.accident` and `dbo.vehicle`

- **Python scripts** perform preprocessing to:
  - Standardize column names and table structures across years
  - Clean data by handling missing values, fixing data types, and decoding coded fields
  - Write cleaned files to Azure Blob for ADF ingestion

- **dbt** performs further transformations inside Azure SQL to:
  - Define staging, intermediate, and data mart models
  - Create optimized tables and views for analytics
  - Apply data quality and integrity tests

### 3. **Visualization**

- **Power BI** connects to Azure SQL reporting objects built from the dbt models
- Reports include:
  - Fatalities by conditions and demographics
  - Summarizes accidents by locality and vehicle types
  - Alcohol related accidents with trend analysis over multiple years

---

## Tools & Technologies

| Tool                     | Role                                                                               |
| ------------------------ | ---------------------------------------------------------------------------------- |
| **Python**               | Handles ingestion, cleaning, and initial transformation of raw CSVs                |
| **Azure Blob**           | Stores raw CSV file data                                                           |
| **Azure Data Factory**   | Loads cleaned files from Blob into Azure SQL staging and executes merge procedures |
| **Microsoft SQL Server** | Stores staged, integrated, and transformed data in Azure                           |
| **dbt Core**             | Builds analysis-ready models and performs data testing                             |
| **Power BI**             | Builds interactive reports and dashboards from the curated MSSQL reporting schema  |
| **Apache Airflow**       | Can orchestrate the ELT and downstream modeling workflow                           |
| **Docker**               | Supports local orchestration and development tooling                               |

---

## Current Architecture

1. Python downloads and cleans FARS data.
2. Cleaned CSV files are published to Azure Blob.
3. Azure Data Factory copies those files into Azure SQL staging tables.
4. Stored procedures merge staged rows into `dbo.accident` and `dbo.vehicle`.
5. dbt transforms Azure SQL source tables into analytics-ready models.
6. Power BI reads from the modeled warehouse.

## Repository Notes

- [`elt/main.py`](c:\Users\Goldb\Desktop\Repos\NHTSA-analytics-workflow\elt\main.py) now stops at the ADF handoff rather than bulk-inserting directly into Azure SQL.
- [`elt/bootstrap_adf_assets.py`](c:\Users\Goldb\Desktop\Repos\NHTSA-analytics-workflow\elt\bootstrap_adf_assets.py) bootstraps the Azure SQL tables, staging tables, and stored procedures used by ADF.

## Dashboard Link: [link](powerbi_link)
