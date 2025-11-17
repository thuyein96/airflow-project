# Start from the official Airflow image version specified in your docker-compose.yaml
ARG AIRFLOW_VERSION=2.10.4
FROM apache/airflow:${AIRFLOW_VERSION}

# Switch to the root user to copy requirements (needed for permission)
USER root
COPY requirements.txt .

# --- CRITICAL CHANGE HERE ---
# Switch to the Airflow user (${AIRFLOW_UID}) before running pip install
USER ${AIRFLOW_UID} 
RUN pip install --no-cache-dir -r requirements.txt
# No need to switch back, as the entrypoint handles user switching later

# If you had other system-level installs (e.g., apt install), you'd sandwich them 
# between 'USER root' and 'USER ${AIRFLOW_UID}'.