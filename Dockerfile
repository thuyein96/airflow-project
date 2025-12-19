FROM apache/airflow:2.10.4-python3.10

# Copy and install your Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install --no-cache-dir apache-airflow-providers-microsoft-azure

# -----------------------------
# Install Terraform
# -----------------------------
USER root

# Install dependencies and Terraform
RUN apt-get update && apt-get install -y unzip curl && \
    curl -LO https://releases.hashicorp.com/terraform/1.5.7/terraform_1.5.7_linux_amd64.zip && \
    unzip terraform_1.5.7_linux_amd64.zip -d /usr/local/bin && \
    rm terraform_1.5.7_linux_amd64.zip && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


    
# FROM apache/airflow:2.10.4

# # Switch to the root user temporarily to install system dependencies (Terraform)
# USER root

# # Install Terraform (for Debian/Ubuntu based images)
# RUN apt-get update && \
#     apt-get install -y curl unzip && \
#     curl -LO https://releases.hashicorp.com/terraform/1.7.5/terraform_1.7.5_linux_amd64.zip && \
#     unzip terraform_1.7.5_linux_amd64.zip -d /usr/local/bin/ && \
#     rm terraform_1.7.5_linux_amd64.zip && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*

# # Switch back to the Airflow user
# USER airflow
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt




# # ... existing content ...

# # Switch to the root user temporarily to install system dependencies (Terraform)
# USER root

# # Install Terraform (Example for Alpine/Debian base)
# # You may need to adapt these commands based on the Airflow image base OS.
# # For Alpine (common in Airflow images):
# RUN apk add --no-cache curl \
#     && curl -LO https://releases.hashicorp.com/terraform/1.7.5/terraform_1.7.5_linux_amd64.zip \
#     && unzip terraform_1.7.5_linux_amd64.zip -d /usr/bin/ \
#     && rm terraform_1.7.5_linux_amd64.zip


# # Switch back to the Airflow user (${AIRFLOW_UID})
# USER ${AIRFLOW_UID} 
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt