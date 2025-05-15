## Features & Tools Used

- **Data Extraction & Loading:**

  - PostgreSQL database hosted on an Azure VM running in Docker for storing raw and processed FARS data.
  - Automated ETL pipeline scripts extract and load raw data into the database.

- **Data Transformation with dbt:**

  - dbt (data build tool) manages SQL-based transformations, models, and testing on the raw data inside PostgreSQL.
  - Enables version-controlled, modular, and repeatable data transformation workflows.
  - dbt models create clean, analysis-ready datasets consumed by the Superset dashboard.

- **Visualization & Exploration:**

  - Apache Superset dashboard for interactive querying, charting, and reporting.
  - Supports multiple visualizations like bar charts, pie charts, and maps.

- **Containerization & Deployment:**
  - Superset runs inside a Docker container for easy deployment and scaling.
  - Hosted on Render.com to provide a scalable, cloud-based analytics service.

---

## Dashboard Link

### https://nhtsa-fars-elt-dashboard.onrender.com
