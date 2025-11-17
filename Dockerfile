# Start from the official Airflow image version specified in your docker-compose.yaml
ARG AIRFLOW_VERSION=2.10.4
FROM apache/airflow:${AIRFLOW_VERSION}

# Switch to the root user temporarily to install requirements
USER root

# Copy and install your custom requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Switch back to the Airflow user
USER ${AIRFLOW_UID}