import os
import json
import psycopg2
import subprocess
import time
import tempfile
from dotenv import load_dotenv
from os.path import expanduser
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# -------------------------
# Default DAG args
# -------------------------
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# -------------------------
# SSH Helper Functions
# -------------------------

def create_temp_ssh_key():
    """Create a temporary SSH key file with proper permissions"""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='_ssh_key') as tmp_key:
        with open('/opt/airflow/dags/.ssh/id_rsa', 'r') as key_file:
            tmp_key.write(key_file.read())
        tmp_key_path = tmp_key.name
    
    os.chmod(tmp_key_path, 0o600)
    return tmp_key_path

def cleanup_temp_key(key_path):
    """Safely remove temporary SSH key"""
    try:
        if key_path and os.path.exists(key_path):
            os.unlink(key_path)
    except Exception as e:
        print(f"Warning: Could not cleanup temp key: {e}")

def wait_for_ssh(host, ssh_key_path, timeout=300, interval=5):
    """Wait for SSH to be available on a host"""
    print(f"Waiting for SSH on {host}...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            result = subprocess.run([
                'ssh',
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'ConnectTimeout=5',
                '-o', 'BatchMode=yes',
                '-i', ssh_key_path,
                f'ubuntu@{host}',
                'echo "SSH_OK"'
            ], capture_output=True, timeout=10, text=True)
            
            if result.returncode == 0 and 'SSH_OK' in result.stdout:
                print(f"✓ SSH available on {host}")
                return True
        except subprocess.TimeoutExpired:
            pass
        except Exception as e:
            print(f"SSH check error: {e}")
        
        time.sleep(interval)
    
    raise TimeoutError(f"SSH not available on {host} after {timeout}s")

def execute_remote_command(host, ssh_key_path, command, timeout=300):
    """Execute a command on remote host via SSH"""
    result = subprocess.run([
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'ServerAliveInterval=60',
        '-o', 'ServerAliveCountMax=3',
        '-i', ssh_key_path,
        f'ubuntu@{host}',
        command
    ], capture_output=True, timeout=timeout, text=True)
    
    return result

def execute_remote_script(host, ssh_key_path, script_content, timeout=600):
    """Upload and execute a script on remote host"""
    # Create remote script
    script_name = f"/tmp/script_{int(time.time())}.sh"
    
    # Upload script
    result = subprocess.run([
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-i', ssh_key_path,
        f'ubuntu@{host}',
        f'cat > {script_name} && chmod +x {script_name}'
    ], input=script_content.encode(), capture_output=True, timeout=30)
    
    if result.returncode != 0:
        raise Exception(f"Failed to upload script: {result.stderr.decode()}")
    
    # Execute script
    result = subprocess.run([
        'ssh',
        '-o', 'StrictHostKeyChecking=no',
        '-o', 'UserKnownHostsFile=/dev/null',
        '-o', 'ServerAliveInterval=60',
        '-i', ssh_key_path,
        f'ubuntu@{host}',
        f'sudo {script_name}'
    ], capture_output=True, timeout=timeout, text=True)
    
    return result

# -------------------------
# DAG Tasks
# -------------------------

def fetch_cluster_info(**context):
    """Fetch provisioned cluster information from database"""
    resource_id = context['dag_run'].conf.get('resource_id')
    if not resource_id:
        raise ValueError("No resource_id received. Stop DAG run.")

    load_dotenv(expanduser('/opt/airflow/dags/.env'))

    connection = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME")
    )
    cursor = connection.cursor()

    # Get resource and cluster info
    cursor.execute(
        'SELECT "name", "region", "resourceConfigId" FROM "Resources" WHERE id = %s;',
        (resource_id,)
    )
    resource = cursor.fetchone()
    if not resource:
        raise ValueError(f"No resource found for resource_id={resource_id}")
    repoName, region, resourceConfigId = resource

    # Get clusters with provisioned status
    cursor.execute(
        '''SELECT "id", "clusterName", "nodeCount", "clusterEndpoint", "terraformState" 
           FROM "AwsK8sCluster" 
           WHERE "resourceConfigId" = %s AND "provisionStatus" = 'provisioned';''',
        (resourceConfigId,)
    )
    clusters = cursor.fetchall()
    
    if not clusters:
        raise ValueError(f"No provisioned clusters found for resourceConfigId={resourceConfigId}")

    cluster_info_list = []
    for cluster in clusters:
        cluster_id, clusterName, nodeCount, clusterEndpoint, terraformState = cluster
        
        master_info = json.loads(clusterEndpoint) if clusterEndpoint else {}
        state = json.loads(terraformState) if terraformState else {}
        
        # Extract worker IPs from terraform state
        worker_ips = []
        for resource in state.get('resources', []):
            if resource.get('type') == 'aws_instance' and resource.get('name') == 'k3s_worker':
                for instance in resource.get('instances', []):
                    attrs = instance.get('attributes', {})
                    tags = attrs.get('tags', {})
                    if tags.get('ClusterId') == cluster_id:
                        worker_ips.append({
                            'public_ip': attrs.get('public_ip'),
                            'private_ip': attrs.get('private_ip'),
                            'instance_id': attrs.get('id')
                        })
        
        # Extract edge proxy info
        edge_info = None
        for resource in state.get('resources', []):
            if resource.get('type') == 'aws_instance' and resource.get('name') == 'k3s_edge':
                for instance in resource.get('instances', []):
                    attrs = instance.get('attributes', {})
                    edge_info = {
                        'public_ip': attrs.get('public_ip'),
                        'private_ip': attrs.get('private_ip'),
                        'instance_id': attrs.get('id')
                    }
                    break
        
        cluster_info_list.append({
            'id': cluster_id,
            'name': clusterName,
            'node_count': nodeCount,
            'master': master_info,
            'workers': worker_ips,
            'edge': edge_info
        })

    cursor.close()
    connection.close()

    project_name = f"{repoName}-{resource_id[:4]}"
    
    return {
        'resource_id': resource_id,
        'region': region,
        'project_name': project_name,
        'clusters': cluster_info_list
    }

def setup_ssh_keys_on_nodes(cluster_info, **context):
    """Setup SSH keys on all nodes so they can communicate"""
    if isinstance(cluster_info, str):
        cluster_info = json.loads(cluster_info)
    
    ssh_key_path = create_temp_ssh_key()
    
    try:
        # Read both public and private keys
        with open('/opt/airflow/dags/.ssh/id_rsa.pub', 'r') as f:
            public_key = f.read().strip()
        
        with open('/opt/airflow/dags/.ssh/id_rsa', 'r') as f:
            private_key = f.read()
        
        all_nodes = []
        
        # Collect all node IPs
        for cluster in cluster_info['clusters']:
            all_nodes.append(('master', cluster['master']['public_ip']))
            for idx, worker in enumerate(cluster['workers']):
                all_nodes.append((f'worker-{idx}', worker['public_ip']))
        
        # Setup SSH keys on each node
        for node_type, node_ip in all_nodes:
            print(f"Setting up SSH keys on {node_type} ({node_ip})...")
            
            # Wait for SSH to be available
            wait_for_ssh(node_ip, ssh_key_path, timeout=180)
            
            # Setup script
            setup_script = f"""#!/bin/bash
set -e

# Ensure SSH directory exists with correct permissions
mkdir -p /home/ubuntu/.ssh
chmod 700 /home/ubuntu/.ssh

# Add public key to authorized_keys (if not already there)
if ! grep -q "{public_key}" /home/ubuntu/.ssh/authorized_keys 2>/dev/null; then
    echo "{public_key}" >> /home/ubuntu/.ssh/authorized_keys
fi
chmod 600 /home/ubuntu/.ssh/authorized_keys

# Install private key for inter-node communication
cat > /home/ubuntu/.ssh/id_rsa << 'PRIVATE_KEY_EOF'
{private_key}PRIVATE_KEY_EOF

chmod 600 /home/ubuntu/.ssh/id_rsa

# Create SSH config to skip host checking for internal network
cat > /home/ubuntu/.ssh/config << 'SSH_CONFIG_EOF'
Host 10.*
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
    ConnectTimeout 10
    ServerAliveInterval 60
    ServerAliveCountMax 3
SSH_CONFIG_EOF

chmod 600 /home/ubuntu/.ssh/config
chown -R ubuntu:ubuntu /home/ubuntu/.ssh

echo "SSH keys configured successfully"
"""
            
            result = execute_remote_script(node_ip, ssh_key_path, setup_script, timeout=60)
            
            if result.returncode != 0:
                print(f"Warning: SSH setup on {node_type} had issues: {result.stderr}")
            else:
                print(f"✓ SSH keys configured on {node_type}")
        
        return {"status": "success", "nodes_configured": len(all_nodes)}
        
    finally:
        cleanup_temp_key(ssh_key_path)

def configure_master_nodes(cluster_info, **context):
    """Install K3s on master nodes"""
    if isinstance(cluster_info, str):
        cluster_info = json.loads(cluster_info)
    
    ssh_key_path = create_temp_ssh_key()
    results = []
    
    try:
        for cluster in cluster_info['clusters']:
            cluster_id = cluster['id']
            cluster_name = cluster['name']
            master_ip = cluster['master']['public_ip']
            
            print(f"Configuring master for cluster: {cluster_name} ({master_ip})")
            
            # Installation script
            install_script = f"""#!/bin/bash
set -ex
exec > >(tee /var/log/k3s-master-install.log) 2>&1

echo "Starting K3s master installation at $(date)"

# Get public IP for TLS SAN
PUBLIC_IP=$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4)
echo "Public IP: $PUBLIC_IP"

# Install K3s server
echo "Installing K3s server..."
curl -sfL https://get.k3s.io | sh -s - server \\
  --write-kubeconfig-mode 644 \\
  --tls-san "$PUBLIC_IP" \\
  --node-taint "node-role.kubernetes.io/control-plane=true:NoSchedule"

# Wait for K3s to be fully ready
echo "Waiting for K3s to be ready..."
until systemctl is-active --quiet k3s; do
  echo "K3s service not active yet..."
  sleep 2
done

until [ -f /var/lib/rancher/k3s/server/node-token ]; do
  echo "Token file not found yet..."
  sleep 2
done

echo "K3s is ready"

# Setup kubeconfig for ubuntu user
mkdir -p /home/ubuntu/.kube
cp /etc/rancher/k3s/k3s.yaml /home/ubuntu/.kube/config
sed -i "s/127.0.0.1/$PUBLIC_IP/g" /home/ubuntu/.kube/config
chown -R ubuntu:ubuntu /home/ubuntu/.kube

# Wait a bit more for API server
sleep 15

# Patch Traefik to NodePort
echo "Configuring Traefik..."
/usr/local/bin/kubectl --kubeconfig=/etc/rancher/k3s/k3s.yaml \\
  -n kube-system patch svc traefik --type merge \\
  -p '{{"spec":{{"type":"NodePort","ports":[{{"name":"web","port":80,"protocol":"TCP","targetPort":8000,"nodePort":30080}},{{"name":"websecure","port":443,"protocol":"TCP","targetPort":8443,"nodePort":30443}}]}}}}'

echo "Master configuration complete at $(date)"
"""
            
            try:
                result = execute_remote_script(master_ip, ssh_key_path, install_script, timeout=300)
                
                if result.returncode != 0:
                    raise Exception(f"Master install failed: {result.stderr}")
                
                print(f"✓ Master configured: {cluster_name}")
                print(f"Output: {result.stdout[-500:]}")  # Last 500 chars
                
                results.append({
                    'cluster_id': cluster_id,
                    'cluster_name': cluster_name,
                    'status': 'configured'
                })
                
            except Exception as e:
                print(f"✗ Failed to configure master {cluster_name}: {str(e)}")
                results.append({
                    'cluster_id': cluster_id,
                    'cluster_name': cluster_name,
                    'status': 'failed',
                    'error': str(e)
                })
        
        return results
        
    finally:
        cleanup_temp_key(ssh_key_path)

def configure_worker_nodes(cluster_info, **context):
    """Install K3s agent on worker nodes using SSH to fetch token"""
    if isinstance(cluster_info, str):
        cluster_info = json.loads(cluster_info)
    
    ssh_key_path = create_temp_ssh_key()
    results = []
    
    try:
        for cluster in cluster_info['clusters']:
            cluster_id = cluster['id']
            cluster_name = cluster['name']
            master_public_ip = cluster['master']['public_ip']
            master_private_ip = cluster['master']['private_ip']
            
            print(f"\nConfiguring workers for cluster: {cluster_name}")
            
            for idx, worker in enumerate(cluster['workers']):
                worker_ip = worker['public_ip']
                
                print(f"  Configuring worker {idx} ({worker_ip})...")
                
                # Worker installation script
                install_script = f"""#!/bin/bash
set -ex
exec > >(tee /var/log/k3s-worker-install.log) 2>&1

echo "Starting K3s worker installation at $(date)"

MASTER_PRIVATE_IP="{master_private_ip}"
MASTER_PUBLIC_IP="{master_public_ip}"

echo "Master private IP: $MASTER_PRIVATE_IP"
echo "Master public IP: $MASTER_PUBLIC_IP"

# Function to fetch token via SSH
fetch_token_via_ssh() {{
    local master_ip=$1
    echo "Attempting to fetch token from $master_ip..."
    
    TOKEN=$(ssh -o ConnectTimeout=10 \\
                -o StrictHostKeyChecking=no \\
                -o UserKnownHostsFile=/dev/null \\
                -i /home/ubuntu/.ssh/id_rsa \\
                ubuntu@$master_ip \\
                "sudo cat /var/lib/rancher/k3s/server/node-token" 2>/dev/null || true)
    
    echo "$TOKEN"
}}

# Wait for master to be SSH accessible and token available
echo "Waiting for master and token..."
TOKEN=""
MAX_ATTEMPTS=60
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    ATTEMPT=$((ATTEMPT + 1))
    echo "Attempt $ATTEMPT/$MAX_ATTEMPTS"
    
    # First check SSH connectivity to master
    if ssh -o ConnectTimeout=5 \\
           -o StrictHostKeyChecking=no \\
           -o UserKnownHostsFile=/dev/null \\
           -i /home/ubuntu/.ssh/id_rsa \\
           ubuntu@$MASTER_PRIVATE_IP \\
           "echo 'SSH_OK'" 2>/dev/null | grep -q "SSH_OK"; then
        
        echo "  SSH connection to master: OK"
        
        # Now try to fetch the token
        TOKEN=$(fetch_token_via_ssh "$MASTER_PRIVATE_IP")
        
        # Validate token (should be K10... format and long)
        if [ -n "$TOKEN" ] && [ "${{#TOKEN}}" -gt 50 ] && echo "$TOKEN" | grep -q "K10"; then
            echo "✓ Token retrieved successfully"
            break
        else
            echo "  Token not ready yet or invalid"
        fi
    else
        echo "  Cannot connect to master via SSH yet"
    fi
    
    if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
        echo "✗ Failed to retrieve token after $MAX_ATTEMPTS attempts"
        exit 1
    fi
    
    sleep 10
done

# Verify master API is accessible
echo "Verifying master K3s API..."
MAX_API_ATTEMPTS=30
API_ATTEMPT=0

while [ $API_ATTEMPT -lt $MAX_API_ATTEMPTS ]; do
    API_ATTEMPT=$((API_ATTEMPT + 1))
    
    if timeout 5 bash -c "cat < /dev/null > /dev/tcp/$MASTER_PRIVATE_IP/6443" 2>/dev/null; then
        echo "✓ Master K3s API is reachable"
        break
    fi
    
    if [ $API_ATTEMPT -eq $MAX_API_ATTEMPTS ]; then
        echo "✗ Master API not reachable after $MAX_API_ATTEMPTS attempts"
        exit 1
    fi
    
    echo "API not ready yet (attempt $API_ATTEMPT/$MAX_API_ATTEMPTS)..."
    sleep 5
done

# Join the cluster
echo "Joining K3s cluster..."
curl -sfL https://get.k3s.io | \\
  K3S_URL="https://$MASTER_PRIVATE_IP:6443" \\
  K3S_TOKEN="$TOKEN" \\
  sh -s - agent

# Wait for agent to start
sleep 10

# Verify agent is running
if systemctl is-active --quiet k3s-agent; then
    echo "✓ K3s agent is running"
else
    echo "✗ K3s agent failed to start"
    systemctl status k3s-agent
    exit 1
fi

# Cleanup private key for security
rm -f /home/ubuntu/.ssh/id_rsa

echo "Worker configuration complete at $(date)"
"""
                
                try:
                    result = execute_remote_script(worker_ip, ssh_key_path, install_script, timeout=900)
                    
                    if result.returncode != 0:
                        raise Exception(f"Worker install failed: {result.stderr}")
                    
                    print(f"  ✓ Worker {idx} configured successfully")
                    
                    results.append({
                        'cluster_id': cluster_id,
                        'worker_index': idx,
                        'status': 'configured'
                    })
                    
                except Exception as e:
                    print(f"  ✗ Failed to configure worker {idx}: {str(e)}")
                    results.append({
                        'cluster_id': cluster_id,
                        'worker_index': idx,
                        'status': 'failed',
                        'error': str(e)
                    })
        
        return results
        
    finally:
        cleanup_temp_key(ssh_key_path)

def configure_edge_proxy(cluster_info, **context):
    """Configure Traefik edge proxy"""
    if isinstance(cluster_info, str):
        cluster_info = json.loads(cluster_info)
    
    ssh_key_path = create_temp_ssh_key()
    
    try:
        # Get edge proxy info
        edge_ip = cluster_info['clusters'][0]['edge']['public_ip']
        
        print(f"Configuring edge proxy at {edge_ip}...")
        
        # Build dynamic Traefik config
        router_config = ""
        service_config = ""
        
        for cluster in cluster_info['clusters']:
            cluster_id = cluster['id']
            cluster_name = cluster['name']
            master_private_ip = cluster['master']['private_ip']
            
            router_config += f"""
  r_{cluster_id}:
    rule: "HostRegexp(`{{{{app:[a-z0-9-]+}}}}.{cluster_name}.orchestronic.dev`)"
    entryPoints:
      - websecure
    tls:
      certResolver: letsencrypt
    service: s_{cluster_id}
"""
            
            service_config += f"""
  s_{cluster_id}:
    loadBalancer:
      passHostHeader: true
      servers:
        - url: "http://{master_private_ip}:30080"
"""
        
        traefik_script = f"""#!/bin/bash
set -ex
exec > >(tee /var/log/traefik-setup.log) 2>&1

echo "Setting up Traefik edge proxy at $(date)"

# Stop and remove existing Traefik if any
docker stop traefik-edge 2>/dev/null || true
docker rm traefik-edge 2>/dev/null || true

# Create directories
mkdir -p /etc/traefik /letsencrypt
touch /letsencrypt/acme.json
chmod 600 /letsencrypt/acme.json

# Static config
cat > /etc/traefik/traefik.yml << 'EOF'
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
EOF

# Dynamic config
cat > /etc/traefik/dynamic.yml << 'EOF'
http:
  routers:{router_config}
  services:{service_config}
EOF

echo "Traefik config files created"
cat /etc/traefik/traefik.yml
cat /etc/traefik/dynamic.yml

# Start Traefik container
docker run -d --restart unless-stopped \\
  --name traefik-edge \\
  -p 80:80 \\
  -p 443:443 \\
  -v /etc/traefik:/etc/traefik:ro \\
  -v /letsencrypt:/letsencrypt \\
  traefik:v2.11

# Wait for container to start
sleep 5

if docker ps | grep -q traefik-edge; then
    echo "✓ Traefik edge proxy started successfully"
    docker logs traefik-edge
else
    echo "✗ Traefik failed to start"
    docker logs traefik-edge
    exit 1
fi

echo "Edge proxy configuration complete at $(date)"
"""
        
        result = execute_remote_script(edge_ip, ssh_key_path, traefik_script, timeout=180)
        
        if result.returncode != 0:
            raise Exception(f"Edge config failed: {result.stderr}")
        
        print(f"✓ Edge proxy configured at {edge_ip}")
        return {'status': 'configured', 'edge_ip': edge_ip}
        
    except Exception as e:
        print(f"✗ Failed to configure edge proxy: {str(e)}")
        return {'status': 'failed', 'error': str(e)}
        
    finally:
        cleanup_temp_key(ssh_key_path)

def fetch_kubeconfigs(cluster_info, **context):
    """Fetch kubeconfig from each master and store in database"""
    if isinstance(cluster_info, str):
        cluster_info = json.loads(cluster_info)
    
    load_dotenv(expanduser('/opt/airflow/dags/.env'))
    ssh_key_path = create_temp_ssh_key()
    
    connection = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME")
    )
    cursor = connection.cursor()
    
    try:
        for cluster in cluster_info['clusters']:
            cluster_id = cluster['id']
            cluster_name = cluster['name']
            master_ip = cluster['master']['public_ip']
            
            print(f"Fetching kubeconfig from {cluster_name} ({master_ip})...")
            
            try:
                # Give K3s some time to settle
                time.sleep(10)
                
                result = execute_remote_command(
                    master_ip,
                    ssh_key_path,
                    'cat /home/ubuntu/.kube/config',
                    timeout=30
                )
                
                if result.returncode != 0:
                    raise Exception(f"Failed to fetch kubeconfig: {result.stderr}")
                
                kubeconfig = result.stdout
                
                if not kubeconfig or len(kubeconfig) < 100:
                    raise Exception("Kubeconfig appears to be empty or invalid")
                
                # Kubeconfig should already have the public IP from our setup script
                # But double-check
                if '127.0.0.1' in kubeconfig or 'localhost' in kubeconfig:
                    kubeconfig = kubeconfig.replace('127.0.0.1', master_ip)
                    kubeconfig = kubeconfig.replace('localhost', master_ip)
                
                # Update database
                cursor.execute(
                    '''UPDATE "AwsK8sCluster" 
                       SET "kubeConfig" = %s, "provisionStatus" = %s
                       WHERE "id" = %s;''',
                    (kubeconfig, 'configured', cluster_id)
                )
                connection.commit()
                
                print(f"✓ Kubeconfig stored for {cluster_name}")
                
            except Exception as e:
                print(f"✗ Failed to fetch kubeconfig for {cluster_name}: {str(e)}")
                # Still mark as configured even if kubeconfig fetch fails
                cursor.execute(
                    'UPDATE "AwsK8sCluster" SET "provisionStatus" = %s WHERE "id" = %s;',
                    ('configured_no_kubeconfig', cluster_id)
                )
                connection.commit()
    
    finally:
        cursor.close()
        connection.close()
        cleanup_temp_key(ssh_key_path)

# -------------------------
# DAG Definition
# -------------------------

with DAG(
    'AWS_configure_k3s',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Configure K3s cluster using SSH (install K3s, join nodes, setup proxy)',
) as dag:

    fetch_info = PythonOperator(
        task_id="fetch_cluster_info",
        python_callable=fetch_cluster_info,
    )

    setup_ssh = PythonOperator(
        task_id="setup_ssh_keys",
        python_callable=setup_ssh_keys_on_nodes,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_cluster_info') }}"],
    )

    config_masters = PythonOperator(
        task_id="configure_master_nodes",
        python_callable=configure_master_nodes,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_cluster_info') }}"],
    )

    config_workers = PythonOperator(
        task_id="configure_worker_nodes",
        python_callable=configure_worker_nodes,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_cluster_info') }}"],
    )

    config_edge = PythonOperator(
        task_id="configure_edge_proxy",
        python_callable=configure_edge_proxy,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_cluster_info') }}"],
    )

    fetch_kubeconfig = PythonOperator(
        task_id="fetch_kubeconfigs",
        python_callable=fetch_kubeconfigs,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_cluster_info') }}"],
    )

    fetch_info >> setup_ssh >> config_masters >> config_workers >> config_edge >> fetch_kubeconfig