#!/bin/bash

# Run DB migrations
superset db upgrade

# Create admin user if it doesn't exist
# Adjust credentials as needed or read from env variables
superset fab create-admin --username admin --password admin --firstname Admin --lastname User --email admin@example.com || true

# Initialize Superset
superset init

# Run Superset server
exec superset run -p 8088 --with-threads --reload --debugger
