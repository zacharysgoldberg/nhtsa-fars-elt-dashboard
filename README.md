# NHTSA FARS ELT Dashboard

This project delivers a complete ELT (Extract, Load, Transform) pipeline and analytics dashboard for the National Highway Traffic Safety Administration's Fatality Analysis Reporting System (FARS). It streamlines data ingestion, transformation, and visualization to provide meaningful insights into traffic fatality trends across the United States.

---

## Project Overview

- Ingests updated FARS data automatically every six months using an Apache Airflow DAG
- Loads raw datasets into a PostgreSQL data warehouse hosted on Azure
- Processes and transforms data using Python scripts for standardization and cleaning
- Models the cleaned data with dbt to power Apache Superset dashboards
- Presents insights using Superset visualizations and dashboards

---

## ELT Workflow

### 1. **Extract & Load**
- Data is sourced from NHTSA’s public FARS CSV datasets.
- An **Apache Airflow** DAG runs every six months to:
  - Check for new FARS data releases
  - Download newly available CSVs
  - Loads the raw data into Azure Blob storage.

### 2. **Transform** (Standardize & Clean with Python)
- Raw CSV files are processed with **Python** to:
  - Normalize column names and table structures across years
  - Convert coded fields into human-readable labels
  - Handle missing values and data type inconsistencies
  - Ensure relational consistency across tables
- The cleaned data is written back to PostgreSQL in a separate schema

### 3. **Model** (Using dbt)
- **dbt** is used to:
  - Define data models (staging, intermediate, marts)
  - Create views and tables optimized for analytical queries
  - Document model lineage and structure for team use
  - Apply tests for data quality and integrity

### 4. **Visualize**
- **Apache Superset** connects to the modeled data schema.
- Dashboards and charts include:
  - Fatalities by state and time
  - Vehicle and driver demographics
  - Trend analysis over multiple years

---

## Tools & Technologies

| Tool            | Role                                                                 |
|-----------------|----------------------------------------------------------------------|
| **Apache Airflow** | Orchestrates and schedules the ELT workflow                        |
| **Python**         | Handles ingestion, cleaning, and transformation of raw CSVs       |
| **PostgreSQL**     | Stores both raw and processed data                                 |
| **dbt**            | Builds analysis-ready models and performs data testing             |
| **Apache Superset**| Hosts interactive dashboards for visualization and exploration     |
| **Docker**         | Containerizes services and manages isolated environments           |


---

## Airflow DAG

- Located in `airflow/dags/`
- Schedule: **Every 6 months**
- Tasks:
  - Check for and download new FARS datasets
  - Run Python ingestion and transformation scripts
  - Load cleaned data into PostgreSQL
  - Trigger dbt models to refresh views/tables
