# NHTSA FARS ELT Dashboard

This project delivers a complete ELT (Extract, Load, Transform) pipeline and analytics dashboard for the National Highway Traffic Safety Administration's (NHTSA) Fatality Analysis Reporting System (FARS) data.

It streamlines data ingestion, transformation, and visualization to provide meaningful insights into traffic fatality trends across the United States.

---

## Project Overview

- Ingests updated FARS data
- Loads raw datasets into a blob storage hosted on Azure
- Processes and transforms data using Python scripts for standardization, cleaning, and dbt for modeling
- Presents insights using Apache Superset dashboards
- Automatically runs pipeline, including dbt models, and updates dashboard every six months using an Apache Airflow DAG

---

## ELT Workflow

### 1. **Extract & Load**

- Data is sourced from NHTSA’s public FARS CSV datasets.
- An **Apache Airflow** DAG runs every six months to:
  - Check for new FARS data releases
  - Download newly available CSVs
  - Load the raw data into Azure Blob storage
  - Transform raw data using Python scripts
  - Finally, updates dashboard by running dbt model transformations

### 2. **Transform**

- **Python scripts** perform initial processing to:

  - Standardizes column names and table structures across years
  - Cleans data by handling missing values, fixing data types, and decoding coded fields
  - Writes cleaned data to Azure Blob as combined CSV files, and PostgreSQL for analysis

- **dbt** performs further transformations inside PostgreSQL to:
  - Define staging, intermediate, and data mart models
  - Create optimized tables and views for analytics
  - Apply data quality and integrity tests

### 3. **Visualization**

- **Apache Superset** connects to the modeled database schema
- Dashboards include:
  - Fatalities by conditions and demographics
  - Summarizes accidents by locality and vehicle types
  - Alcohol related accidents with trend analysis over multiple years

---

## Tools & Technologies

| Tool                | Role                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------ |
| **Apache Airflow**  | Orchestrates and schedules the ELT workflow                                                      |
| **Python**          | Handles ingestion, cleaning, and initial transformation of raw CSVs                              |
| **Azure Blob**      | Stores raw CSV file data                                                                         |
| **PostgreSQL**      | Stores processed and transformed data                                                            |
| **dbt**             | Builds analysis-ready models and performs data testing                                           |
| **Apache Superset** | Hosts interactive dashboards for visualization and exploration                                   |
| **Docker**          | Containerizes Superset, Airflow, and Postgres databases for both FARS data and Superset metadata |

---

## Dashboard Link and Sign In: [Superset Dashboard](https://nhtsa-fars-elt-dashboard.onrender.com/superset/dashboard/p/QaKezOZEDkd/)

To view the dashboard and metrics, **sign in using the public credentials below**:

- **Username:** `User`
- **Password:** `password`

> ⚠️ **Note:** It may take several minutes for Render to spin up Apache Superset. Please be patient!
