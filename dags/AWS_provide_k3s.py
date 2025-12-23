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
    
    try:
        with open('/opt/airflow/dags/.ssh/id_rsa.pub', 'r') as pub_key_file:
            ssh_public_key = pub_key_file.read().strip()
    except FileNotFoundError:
        raise ValueError("Public key not found at /opt/airflow/dags/.ssh/id_rsa.pub")

    # main.tf
    main_tf_content = f"""
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

# Define the Key Pair
resource "aws_key_pair" "k3s_auth" {{
  key_name   = "${{var.project_name}}-key"
  public_key = "{ssh_public_key}"
}}

# VPC for K3s
resource "aws_vpc" "k3s_vpc" {{
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  
  tags = {{
    Name = "${{var.project_name}}-k3s-vpc"
  }}
}}

# Internet Gateway
resource "aws_internet_gateway" "k3s_igw" {{
  vpc_id = aws_vpc.k3s_vpc.id
  
  tags = {{
    Name = "${{var.project_name}}-k3s-igw"
  }}
}}

# Subnets
data "aws_availability_zones" "available" {{
  state = "available"
}}

resource "aws_subnet" "k3s_subnet" {{
  count                   = 2
  vpc_id                  = aws_vpc.k3s_vpc.id
  cidr_block              = "10.0.${{count.index}}.0/24"
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true
  
  tags = {{
    Name = "${{var.project_name}}-k3s-subnet-${{count.index}}"
  }}
}}

# Route Table
resource "aws_route_table" "k3s_rt" {{
  vpc_id = aws_vpc.k3s_vpc.id
  
  route {{
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.k3s_igw.id
  }}
  
  tags = {{
    Name = "${{var.project_name}}-k3s-rt"
  }}
}}

resource "aws_route_table_association" "k3s_rta" {{
  count          = 2
  subnet_id      = aws_subnet.k3s_subnet[count.index].id
  route_table_id = aws_route_table.k3s_rt.id
}}

# Security Group for K3s
resource "aws_security_group" "k3s_sg" {{
  name   = "${{var.project_name}}-k3s-sg"
  vpc_id = aws_vpc.k3s_vpc.id

  # Allow SSH
  ingress {{
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }}

  # Allow K3s API (6443)
  ingress {{
    from_port   = 6443
    to_port     = 6443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }}

  # Allow HTTP (80)
  ingress {{
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }}

  # Allow HTTPS (443)
  ingress {{
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }}

  # Allow internal VPC traffic
  ingress {{
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }}

  # Allow all outbound traffic
  egress {{
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }}

  tags = {{
    Name = "${{var.project_name}}-k3s-sg"
  }}
}}

# IAM Role for K3s nodes
resource "aws_iam_role" "k3s_role" {{
  name = "${{var.project_name}}-k3s-role"

  assume_role_policy = jsonencode({{
    Version = "2012-10-17"
    Statement = [{{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {{
        Service = "ec2.amazonaws.com"
      }}
    }}]
  }})
}}

# IAM instance profile
resource "aws_iam_instance_profile" "k3s_profile" {{
  name = "${{var.project_name}}-k3s-profile"
  role = aws_iam_role.k3s_role.name
}}

# Attach policy for SSM access (optional, for Systems Manager access)
resource "aws_iam_role_policy_attachment" "k3s_ssm_policy" {{
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
  role       = aws_iam_role.k3s_role.name
}}

# Get latest Ubuntu 22.04 LTS AMI
data "aws_ami" "ubuntu" {{
  most_recent = true
  owners      = ["099720109477"] # Canonical

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
  instance_type          = each.value.node_size
  iam_instance_profile   = aws_iam_instance_profile.k3s_profile.name
  subnet_id              = aws_subnet.k3s_subnet[0].id
  vpc_security_group_ids = [aws_security_group.k3s_sg.id]
  
  associate_public_ip_address = true
  
  key_name = aws_key_pair.k3s_auth.key_name

  # K3s master installation script
  user_data = base64encode(<<-EOF
              #!/bin/bash
              set -e
              apt-get update
              apt-get install -y curl wget git
              
              # Install K3s master (keep Traefik enabled)
              curl -sfL https://get.k3s.io | sh -s - --write-kubeconfig-mode 644
              
              # Wait for K3s to be ready
              sleep 15
              
              # Save kubeconfig
              mkdir -p /home/ubuntu/.kube
              sudo cp /etc/rancher/k3s/k3s.yaml /home/ubuntu/.kube/config
              sudo chown ubuntu:ubuntu /home/ubuntu/.kube/config
              
              # Wait for kubectl to be available
              sleep 10
              until /usr/local/bin/kubectl --kubeconfig=/etc/rancher/k3s/k3s.yaml get nodes &> /dev/null; do
                sleep 5
              done
              
              # Install MetalLB
              /usr/local/bin/kubectl --kubeconfig=/etc/rancher/k3s/k3s.yaml apply -f https://raw.githubusercontent.com/metallb/metallb/v0.13.10/config/manifests/namespace.yaml
              /usr/local/bin/kubectl --kubeconfig=/etc/rancher/k3s/k3s.yaml apply -f https://raw.githubusercontent.com/metallb/metallb/v0.13.10/config/manifests/metallb.yaml
              
              # Wait for MetalLB to be ready
              sleep 10
              
              # Configure MetalLB with IP pool
              cat <<'METALLB_CONFIG' | /usr/local/bin/kubectl --kubeconfig=/etc/rancher/k3s/k3s.yaml apply -f -
              apiVersion: metallb.io/v1beta1
              kind: IPAddressPool
              metadata:
                name: first-pool
                namespace: metallb-system
              spec:
                addresses:
                - 10.0.0.100-10.0.0.200
              ---
              apiVersion: metallb.io/v1beta1
              kind: L2Advertisement
              metadata:
                name: example
                namespace: metallb-system
              METALLB_CONFIG
              
              # Verify CoreDNS is running (CoreDNS comes with K3s by default)
              sleep 5
              /usr/local/bin/kubectl --kubeconfig=/etc/rancher/k3s/k3s.yaml get pods -n kube-system | grep coredns
              
              # Verify Traefik is running
              /usr/local/bin/kubectl --kubeconfig=/etc/rancher/k3s/k3s.yaml get pods -n kube-system | grep traefik
              
              # Get node token for workers
              cat /var/lib/rancher/k3s/server/node-token > /tmp/k3s-token.txt
              EOF
  )
  
  tags = {{
    Name = "${{each.value.cluster_name}}-master"
    ClusterName = each.value.cluster_name
  }}

  depends_on = [aws_internet_gateway.k3s_igw]
}}

# K3s Worker Nodes
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
  subnet_id              = aws_subnet.k3s_subnet[each.value.index % 2].id
  vpc_security_group_ids = [aws_security_group.k3s_sg.id]
  
  associate_public_ip_address = true
  key_name = aws_key_pair.k3s_auth.key_name

  # K3s worker installation script
  user_data = base64encode(<<-EOF
              #!/bin/bash
              set -e
              apt-get update
              apt-get install -y curl wget git
              
              # Wait for master to be ready
              sleep 30
              
              # Get master IP
              MASTER_IP="${"$"}{{aws_instance.k3s_master[each.value.cluster_id].private_ip}}"
              
              # Get node token from master
              aws s3 cp s3://k3s-config-${{var.project_name}}/k3s-token.txt /tmp/k3s-token.txt || echo "Token not found, using default"
              
              # Install K3s worker
              export K3S_URL=https://$MASTER_IP:6443
              export K3S_TOKEN="$(cat /tmp/k3s-token.txt 2>/dev/null || echo 'default-token')"
              curl -sfL https://get.k3s.io | K3S_URL=$K3S_URL K3S_TOKEN=$K3S_TOKEN sh -
              EOF
  )
  
  tags = {{
    Name = "${{each.value.cluster_name}}-worker-${{each.value.index}}"
    ClusterName = each.value.cluster_name
  }}

  depends_on = [aws_instance.k3s_master]
}}

# Output cluster information
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

    with open(f"{terraform_dir}/main.tf", "w") as f:
        f.write(main_tf_content)

    # variables.tf
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
