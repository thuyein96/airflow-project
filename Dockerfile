# ... existing content ...

# Switch to the root user temporarily to install system dependencies (Terraform)
USER root

# Install Terraform (Example for Alpine/Debian base)
# You may need to adapt these commands based on the Airflow image base OS.
# For Alpine (common in Airflow images):
# RUN apk add --no-cache curl \
#     && curl -LO https://releases.hashicorp.com/terraform/1.7.5/terraform_1.7.5_linux_amd64.zip \
#     && unzip terraform_1.7.5_linux_amd64.zip -d /usr/bin/ \
#     && rm terraform_1.7.5_linux_amd64.zip
# For Debian/Ubuntu based images:
RUN apt-get update && \
    apt-get install -y curl unzip && \
    curl -LO https://releases.hashicorp.com/terraform/1.7.5/terraform_1.7.5_linux_amd64.zip && \
    unzip terraform_1.7.5_linux_amd64.zip -d /usr/local/bin/ && \
    rm terraform_1.7.5_linux_amd64.zip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the Airflow user (${AIRFLOW_UID})
USER ${AIRFLOW_UID} 
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt