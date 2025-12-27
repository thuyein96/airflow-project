import ast
import os
import json
import psycopg2
from dotenv import load_dotenv
from os.path import expanduser
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# -------------------------
# Default DAG args
# -------------------------
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

def fetch_from_database(**context):
    resource_id = context['dag_run'].conf.get('resource_id')
    if not resource_id:
        raise ValueError("No resource_id received. Stop DAG run.")

    load_dotenv(expanduser('/opt/airflow/dags/.env'))

    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    DBNAME = os.getenv("DB_NAME")

    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME,
    )
    cursor = connection.cursor()

    # Get resource info directly from Resources table
    cursor.execute(
        'SELECT "name", "region", "resourceConfigId" FROM "Resources" WHERE id = %s;',
        (resource_id,)
    )
    resource = cursor.fetchone()
    if not resource:
        raise ValueError(f"No resource found for resource_id={resource_id}")
    repoName, region, resourceConfigId = resource

    # Get K3s cluster configurations
    cursor.execute(
        'SELECT "id", "clusterName", "nodeCount", "nodeSize" '
        'FROM "AwsK8sCluster" WHERE "resourceConfigId" = %s;',
        (resourceConfigId,)
    )
    k3s_clusters = cursor.fetchall()
    if not k3s_clusters:
        raise ValueError(f"No K3s cluster found for resourceConfigId={resourceConfigId}")

    cluster_list = []
    for cluster in k3s_clusters:
        id, clusterName, nodeCount, nodeSize = cluster
        cluster_list.append({
            "id": id,
            "cluster_name": clusterName,
            "node_count": nodeCount,
            "node_size": nodeSize
        })

    cursor.close()
    connection.close()

    configInfo = {
        "resourcesId": resource_id,
        "repoName": repoName,
        "region": region,
        "k3s_clusters": cluster_list
    }
    return configInfo

def create_terraform_directory(configInfo):
    if isinstance(configInfo, str):
        configInfo = ast.literal_eval(configInfo)
        
    repoName = configInfo['repoName']
    terraform_dir = f"/opt/airflow/dags/terraform/{repoName}/k3s"
    os.makedirs(terraform_dir, exist_ok=True)
    return terraform_dir

def write_terraform_files(terraform_dir, configInfo):
    if isinstance(configInfo, str):
        configInfo = ast.literal_eval(configInfo)
        
    config_dict = configInfo
    projectName = f"{config_dict['repoName']}-{config_dict['resourcesId'][:4]}"
    k3s_clusters = config_dict['k3s_clusters']

    load_dotenv(expanduser('/opt/airflow/dags/.env'))

    # terraform.auto.tfvars
    tfvars_content = f"""
access_key       = "{os.getenv('AWS_ACCESS_KEY')}"
secret_key       = "{os.getenv('AWS_SECRET_KEY')}"
project_location = "{config_dict['region']}"
project_name     = "{projectName}"
k3s_clusters     = {json.dumps(k3s_clusters, indent=4)}
"""
    with open(f"{terraform_dir}/terraform.auto.tfvars", "w") as f:
        f.write(tfvars_content)
    
    # 1. READ PUBLIC KEY
    try:
        with open('/opt/airflow/dags/.ssh/id_rsa.pub', 'r') as pub_key_file:
            ssh_public_key = pub_key_file.read().strip()
    except FileNotFoundError:
        raise ValueError("Public key not found at /opt/airflow/dags/.ssh/id_rsa.pub")

    # 2. READ PRIVATE KEY (New Addition)
    # We need the private key to inject into the worker node so it can SSH to the master
    try:
        with open('/opt/airflow/dags/.ssh/id_rsa', 'r') as priv_key_file:
            ssh_private_key = priv_key_file.read().strip()
    except FileNotFoundError:
        raise ValueError("Private key not found at /opt/airflow/dags/.ssh/id_rsa")

    # main.tf Template
    main_tf_template = """
terraform {{
  required_providers {{
    aws = {{
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }}
  }}
}}

provider "aws" {{
  region     = var.project_location
  access_key = var.access_key
  secret_key = var.secret_key
}}

# S3 Bucket (Optional now, but kept to prevent state conflict if removing)
resource "aws_s3_bucket" "k3s_token" {{
  bucket        = "k3s-config-${{var.project_name}}"
  force_destroy = true
  tags = {{ Name = "k3s-config-${{var.project_name}}" }}
}}

resource "aws_key_pair" "k3s_auth" {{
  key_name   = "${{var.project_name}}-key"
  public_key = "__SSH_PUBLIC_KEY__"
}}

# DATA LOOKUPS (VPC/SUBNETS)
data "aws_vpc" "k3s_vpc" {{
  filter {{
    name   = "tag:Name"
    values = ["${{var.project_name}}-k3s-vpc"]
  }}
}}

data "aws_subnet" "k3s_subnet_0" {{
  filter {{
    name   = "tag:Name"
    values = ["${{var.project_name}}-k3s-subnet-0"]
  }}
}}

data "aws_subnet" "k3s_subnet_1" {{
  filter {{
    name   = "tag:Name"
    values = ["${{var.project_name}}-k3s-subnet-1"]
  }}
}}

# SECURITY GROUPS
resource "aws_security_group" "k3s_sg" {{
  name   = "${{var.project_name}}-k3s-sg"
  vpc_id = data.aws_vpc.k3s_vpc.id

  ingress {{
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }}
  ingress {{
    from_port   = 6443
    to_port     = 6443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }}
  ingress {{
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }}
  egress {{
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }}
  tags = {{ Name = "${{var.project_name}}-k3s-sg" }}
}}

resource "aws_security_group" "edge_sg" {{
  name   = "${{var.project_name}}-edge-sg"
  vpc_id = data.aws_vpc.k3s_vpc.id
  ingress {{
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }}
  ingress {{
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }}
  ingress {{
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }}
  egress {{
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }}
  tags = {{ Name = "${{var.project_name}}-edge-sg" }}
}}

# IAM Role
resource "aws_iam_role" "k3s_role" {{
  name = "${{var.project_name}}-k3s-role"
  assume_role_policy = jsonencode({{
    Version = "2012-10-17"
    Statement = [{{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {{ Service = "ec2.amazonaws.com" }}
    }}]
  }})
}}

resource "aws_iam_instance_profile" "k3s_profile" {{
  name = "${{var.project_name}}-k3s-profile"
  role = aws_iam_role.k3s_role.name
}}

resource "aws_iam_role_policy_attachment" "k3s_ssm_policy" {{
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
  role       = aws_iam_role.k3s_role.name
}}

data "aws_ami" "ubuntu" {{
  most_recent = true
  owners      = ["099720109477"]
  filter {{
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }}
  filter {{
    name   = "virtualization-type"
    values = ["hvm"]
  }}
}}

# K3s Master Node
resource "aws_instance" "k3s_master" {{
  for_each = {{ for cluster in var.k3s_clusters : cluster.id => cluster }}
  
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t3.medium"
  iam_instance_profile   = aws_iam_instance_profile.k3s_profile.name
  subnet_id              = data.aws_subnet.k3s_subnet_0.id
  vpc_security_group_ids = [aws_security_group.k3s_sg.id]
  associate_public_ip_address = true
  key_name = aws_key_pair.k3s_auth.key_name

  user_data = base64encode(<<-EOF
              #!/bin/bash
              set -e
              apt-get update
              apt-get install -y curl wget git awscli
              
              PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)

              # Install K3s server
              curl -sfL https://get.k3s.io | sh -s - server --write-kubeconfig-mode 644 --tls-san "$PUBLIC_IP" --node-taint "node-role.kubernetes.io/control-plane=true:NoSchedule"
              
              sleep 15
              mkdir -p /home/ubuntu/.kube
              cp /etc/rancher/k3s/k3s.yaml /home/ubuntu/.kube/config
              chown ubuntu:ubuntu /home/ubuntu/.kube/config
              
              # Traefik NodePort Patching
              sleep 10
              /usr/local/bin/kubectl --kubeconfig=/etc/rancher/k3s/k3s.yaml -n kube-system patch svc traefik --type merge -p '{"spec":{"type":"NodePort","ports":[{"name":"web","port":80,"protocol":"TCP","targetPort":8000,"nodePort":30080},{"name":"websecure","port":443,"protocol":"TCP","targetPort":8443,"nodePort":30443}]}}'
              EOF
  )
  
  tags = {{
    Name = "${{each.value.cluster_name}}-master"
    ClusterName = each.value.cluster_name
  }}
}}

# Edge Proxy
resource "aws_instance" "k3s_edge" {{
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "t3.micro"
  iam_instance_profile   = aws_iam_instance_profile.k3s_profile.name
  subnet_id              = data.aws_subnet.k3s_subnet_0.id
  vpc_security_group_ids = [aws_security_group.edge_sg.id]
  associate_public_ip_address = true
  key_name = aws_key_pair.k3s_auth.key_name

  user_data = base64encode(<<-EOF
              #!/bin/bash
              set -e
              apt-get update
              apt-get install -y ca-certificates curl gnupg docker.io
              
              # (Shortened for brevity, keep your original Traefik config logic here)
              mkdir -p /etc/traefik /letsencrypt
              touch /letsencrypt/acme.json && chmod 600 /letsencrypt/acme.json

              # Static config (TLS terminated at edge with Let's Encrypt)
              cat > /etc/traefik/traefik.yml <<'TRAEFIK_STATIC'
              entryPoints:
                web:
                  address: ":80"
                  http:
                    redirections:
                      entryPoint:
                        to: websecure
                        scheme: https
                websecure:
                  address: ":443"
              providers:
                file:
                  filename: /etc/traefik/dynamic.yml
                  watch: true
              certificatesResolvers:
                letsencrypt:
                  acme:
                    email: "admin@orchestronic.dev"
                    storage: /letsencrypt/acme.json
                    httpChallenge:
                      entryPoint: web
              log:
                level: INFO
              TRAEFIK_STATIC

              # Dynamic config: route per cluster by host pattern to that cluster's Traefik NodePort (30080)
              cat > /etc/traefik/dynamic.yml <<'TRAEFIK_DYNAMIC'
              http:
                routers:
%{{ for cluster in var.k3s_clusters ~}}
                  r_${{cluster.id}}:
                    rule: "HostRegexp(`{app:[a-z0-9-]+}.${{cluster.cluster_name}}.orchestronic.dev`)"
                    entryPoints:
                      - websecure
                    tls:
                      certResolver: letsencrypt
                    service: s_${{cluster.id}}
%{{ endfor ~}}
                services:
%{{ for cluster in var.k3s_clusters ~}}
                  s_${{cluster.id}}:
                    loadBalancer:
                      passHostHeader: true
                      servers:
                        - url: "http://${{aws_instance.k3s_master[cluster.id].private_ip}}:30080"
%{{ endfor ~}}
              TRAEFIK_DYNAMIC

              docker run -d --restart unless-stopped \
                --name traefik-edge \
                -p 80:80 -p 443:443 \
                -v /etc/traefik:/etc/traefik:ro \
                -v /letsencrypt:/letsencrypt \
                traefik:v2.11
              
              EOF
  )

  tags = {{ Name = "${{var.project_name}}-edge" }}
  depends_on = [aws_instance.k3s_master]
}}

# K3s Worker Nodes (UPDATED USER DATA)
resource "aws_instance" "k3s_worker" {{
  for_each = {{
    for item in flatten([
      for cluster in var.k3s_clusters : [
        for i in range(cluster.node_count) : {{
          cluster_id = cluster.id
          cluster_name = cluster.cluster_name
          node_size = cluster.node_size
          index = i
        }}
      ]
    ]) : "${{item.cluster_id}}-worker-${{item.index}}" => item
  }}
  
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = each.value.node_size
  iam_instance_profile   = aws_iam_instance_profile.k3s_profile.name
  subnet_id              = each.value.index % 2 == 0 ? data.aws_subnet.k3s_subnet_0.id : data.aws_subnet.k3s_subnet_1.id
  vpc_security_group_ids = [aws_security_group.k3s_sg.id]
  associate_public_ip_address = true
  key_name = aws_key_pair.k3s_auth.key_name

  # K3s worker installation script using SSH to fetch token
  user_data = base64encode(<<-EOF
              #!/bin/bash
              set -e
              apt-get update
              apt-get install -y curl wget git awscli
              
              # 1. SETUP SSH KEYS
              # We inject the private key so this worker can SSH to the master
              mkdir -p /home/ubuntu/.ssh
cat > /home/ubuntu/.ssh/id_rsa <<'KEYEOF'
__SSH_PRIVATE_KEY__
KEYEOF
              chmod 600 /home/ubuntu/.ssh/id_rsa
              chown ubuntu:ubuntu /home/ubuntu/.ssh/id_rsa
              
              # Wait for master to be ready
              sleep 30
              
              # Get master private IP (Terraform will render the actual IP here)
              MASTER_IP="${{aws_instance.k3s_master[each.value.cluster_id].private_ip}}"

              # 2. FETCH TOKEN VIA SSH
              TOKEN=""
              for i in $(seq 1 60); do
                # Try to cat the token file from the master
                TOKEN=$(ssh -o StrictHostKeyChecking=no -i /home/ubuntu/.ssh/id_rsa ubuntu@$MASTER_IP "sudo cat /var/lib/rancher/k3s/server/node-token" 2>/dev/null || true)
                
                if [ -n "$TOKEN" ]; then
                  echo "Got token."
                  break
                fi
                echo "Waiting for token..."
                sleep 5
              done
              
              if [ -z "$TOKEN" ]; then
                echo "Failed to fetch K3s token from $MASTER_IP via SSH" >&2
                exit 1
              fi

              # 3. JOIN CLUSTER
              curl -sfL https://get.k3s.io | K3S_URL="https://$MASTER_IP:6443" K3S_TOKEN="$TOKEN" sh -s - agent
              
              # Clean up key for security
              rm -f /home/ubuntu/.ssh/id_rsa
              EOF
  )
  
  tags = {{
    Name = "${{each.value.cluster_name}}-worker-${{each.value.index}}"
    ClusterName = each.value.cluster_name
  }}

  depends_on = [aws_instance.k3s_master]
}}

output "k3s_masters" {{
  value = {{
    for cluster_id, master in aws_instance.k3s_master : cluster_id => {{
      public_ip  = master.public_ip
      private_ip = master.private_ip
    }}
  }}
}}

output "k3s_workers" {{
  value = {{
    for worker_id, worker in aws_instance.k3s_worker : worker_id => {{
      public_ip  = worker.public_ip
      private_ip = worker.private_ip
    }}
  }}
}}
"""

    # 3. DO STRING REPLACEMENTS
    # Replace the placeholders with actual keys
    main_tf_content = (
        main_tf_template
        .replace("{{", "{")
        .replace("}}", "}")
        .replace("__SSH_PUBLIC_KEY__", ssh_public_key)
        .replace("__SSH_PRIVATE_KEY__", ssh_private_key)
    )

    with open(f"{terraform_dir}/main.tf", "w") as f:
        f.write(main_tf_content)

    # variables.tf (Unchanged)
    variables_tf = f"""
variable "access_key" {{
  description = "AWS Access Key"
  type        = string
}}
variable "secret_key" {{
  description = "AWS Secret Key"
  type        = string
}}
variable "project_location" {{
  description = "AWS Region"
  type        = string
  default     = "{config_dict['region']}"
}}
variable "project_name" {{
  description = "Project Name"
  type        = string
  default     = "{projectName}"
}}
variable "k3s_clusters" {{
  description = "List of K3s cluster configurations"
  type = list(object({{
    id           = string
    cluster_name = string
    node_count   = number
    node_size    = string
  }}))
}}
"""
    with open(f"{terraform_dir}/variables.tf", "w") as f:
        f.write(variables_tf)

def write_to_db(terraform_dir, configInfo, **context):
    if isinstance(configInfo, str):
        configInfo = ast.literal_eval(configInfo)

    k3s_output_file = Path(terraform_dir) / "terraform.tfstate"
    if not k3s_output_file.exists():
        raise FileNotFoundError(f"Terraform state file not found at {k3s_output_file}")

    load_dotenv(expanduser('/opt/airflow/dags/.env'))

    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    DBNAME = os.getenv("DB_NAME")

    connection = psycopg2.connect(
        user=USER, password=PASSWORD,
        host=HOST, port=PORT, dbname=DBNAME
    )
    cursor = connection.cursor()

    with open(k3s_output_file, 'r') as f:
        k3s_state = json.load(f)

    # Extract master and worker IPs for each cluster from Terraform state
    for cluster in configInfo['k3s_clusters']:
        cluster_id = cluster['id']
        cluster_name = cluster['cluster_name']

        master_public_ip = None
        master_private_ip = None
        # worker_ips = []

        for resource in k3s_state.get('resources', []):
            if resource.get('type') == 'aws_instance' and resource.get('name') == 'k3s_master':
                for instance in resource.get('instances', []):
                    attributes = instance.get('attributes', {})
                    index_key = instance.get('index_key')

                    if index_key == cluster_id:
                        master_public_ip = attributes.get('public_ip', '')
                        master_private_ip = attributes.get('private_ip', '')
                        break

        if not master_public_ip:
            print(f"Warning: No master found in Terraform state for cluster {cluster_id}")
            continue

        # Fetch kubeconfig from master via SSH
        import subprocess
        import time
        import tempfile
        kubeconfig = None
        
        # Create a temporary private key file with correct permissions (600)
        # This is necessary because the mounted key file might have different ownership/permissions
        # that cause SSH to fail or Airflow to be unable to read it.
        tmp_key_path = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_key:
                with open('/opt/airflow/dags/.ssh/id_rsa', 'r') as key_file:
                    tmp_key.write(key_file.read())
                tmp_key_path = tmp_key.name
            
            os.chmod(tmp_key_path, 0o600)

            # Retry loop to wait for K3s installation to complete (up to 5 minutes)
            max_retries = 10
            for attempt in range(max_retries):
                try:
                    print(f"Attempt {attempt+1}/{max_retries} to fetch kubeconfig from {master_public_ip}...")
                    
                    result = subprocess.run(
                        [
                            'ssh',
                            '-o', 'StrictHostKeyChecking=no',
                            '-o', 'UserKnownHostsFile=/dev/null',
                            '-o', 'ConnectTimeout=10', # Fast fail on connection
                            '-i', tmp_key_path,
                            f'ubuntu@{master_public_ip}',
                            # Check if file exists and cat it, otherwise fail
                            'if [ -f /home/ubuntu/.kube/config ]; then cat /home/ubuntu/.kube/config; else exit 1; fi'
                        ],
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    
                    if result.returncode == 0 and result.stdout.strip():
                        kubeconfig = result.stdout
                        # Replace localhost/127.0.0.1 with public IP
                        kubeconfig = kubeconfig.replace('127.0.0.1', master_public_ip)
                        kubeconfig = kubeconfig.replace('localhost', master_public_ip)

                        # Modify kubeconfig to skip TLS verification (solves self-signed cert errors)
                        # import re
                        # kubeconfig = re.sub(r'certificate-authority-data:.*', 'insecure-skip-tls-verify: true', kubeconfig)

                        print(f"✓ Retrieved kubeconfig for cluster {cluster_id}")
                        break
                    else:
                        print(f"  Kubeconfig not ready yet. stderr: {result.stderr.strip() if result.stderr else 'None'}")
                except subprocess.TimeoutExpired:
                    print(f"  SSH connection timed out.")
                except Exception as e:
                    print(f"  Error fetching kubeconfig: {str(e)}")
                
                if attempt < max_retries - 1:
                    print("  Waiting 30 seconds before next attempt...")
                    time.sleep(30)
        finally:
            if tmp_key_path and os.path.exists(tmp_key_path):
                os.unlink(tmp_key_path)

        # Store cluster endpoint and terraform state
        update_query = 'UPDATE "AwsK8sCluster" SET "clusterEndpoint" = %s, "terraformState" = %s'
        params = [json.dumps(master_public_ip), json.dumps(k3s_state)]
        
        # Add kubeconfig if available
        if kubeconfig:
            update_query += ', "kubeConfig" = %s'
            params.append(kubeconfig)
        
        update_query += ' WHERE "id" = %s;'
        params.append(cluster_id)
        
        cursor.execute(update_query, tuple(params))
        connection.commit()
        print(f"✓ Updated cluster {cluster_id} in database")

    connection.commit()
    cursor.close()
    connection.close()

with DAG(
    'AWS_terraform_k3s_provision',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_config",
        python_callable=fetch_from_database,
    )

    create_dir_task = PythonOperator(
        task_id="create_terraform_dir",
        python_callable=create_terraform_directory,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_config') }}"],
    )

    write_files_task = PythonOperator(
        task_id="write_terraform_files",
        python_callable=write_terraform_files,
        op_args=[
            "{{ ti.xcom_pull(task_ids='create_terraform_dir') }}",
            "{{ ti.xcom_pull(task_ids='fetch_config') }}",
        ],
    )

    terraform_init = BashOperator(
        task_id="terraform_init",
        bash_command="cd {{ ti.xcom_pull(task_ids='create_terraform_dir') }} && terraform init",
    )

    terraform_apply = BashOperator(
        task_id="terraform_apply",
        bash_command="cd {{ ti.xcom_pull(task_ids='create_terraform_dir') }} && terraform apply -auto-approve",
    )

    write_to_db_task = PythonOperator(
        task_id="write_to_db",
        python_callable=write_to_db,
        op_args=[
            "{{ ti.xcom_pull(task_ids='create_terraform_dir') }}",
            "{{ ti.xcom_pull(task_ids='fetch_config') }}",
        ],
    )

    terraform_cleanup = BashOperator(
        task_id="terraform_cleanup",
        bash_command=(
            "cd {{ ti.xcom_pull(task_ids='create_terraform_dir') }} && "
            "if [ -f .terraform.tfstate.lock.info ]; then "
            "  LOCK_ID=$(cat .terraform.tfstate.lock.info | grep -oP '(?<=\"ID\":\")[^\"]*') && "
            "  terraform force-unlock -force $LOCK_ID || true; "
            "fi"
        ),
    )

    fetch_task >> create_dir_task >> write_files_task >> terraform_init >> terraform_apply >> write_to_db_task >> terraform_cleanup
