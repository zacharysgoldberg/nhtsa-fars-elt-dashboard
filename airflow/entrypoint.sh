#!/bin/bash

# Exit immediately on error
set -e

# Initialize the Airflow database
airflow db init

# Create admin user if it doesn't already exist
airflow users create \
  --username "$AIRFLOW_ADMIN_USERNAME" \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email "$AIRFLOW_ADMIN_EMAIL" \
  --password "$AIRFLOW_ADMIN_PASSWORD"

# Optional: you can print success message
echo "Airflow DB initialized and user created."
