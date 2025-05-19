#!/bin/bash

# Run DB migrations
superset db upgrade

# Create admin user if it doesn't exist
# Adjust credentials as needed or read from env variables
superset fab create-admin \
  --username "$SUPERSET_ADMIN_USERNAME" \
  --password "$SUPERSET_ADMIN_PASSWORD" \
  --firstname Admin \
  --lastname User \
  --email "$SUPERSET_ADMIN_EMAIL" || true

# Initialize Superset
superset init

# Run Superset server listening on all interfaces
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger
