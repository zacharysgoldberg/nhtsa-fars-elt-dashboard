FROM apache/airflow:2.8.0

# Switch to root to install system packages
USER root

# Install Git (for pulling DAGs or dbt projects)
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python packages (dbt-postgres and psycopg2)
RUN pip install --no-cache-dir dbt-postgres psycopg2-binary

# Copy your custom entrypoint script
COPY ./airflow/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Switch back to airflow user
USER airflow

# Expose Airflow webserver port
EXPOSE 8080

# Use custom entrypoint script
ENTRYPOINT ["/entrypoint.sh"]
