import os
import json
import psycopg2
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

# --------------------------------------------------
# Default DAG args
# --------------------------------------------------
default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

# --------------------------------------------------
# Dynamic Path Configuration
# --------------------------------------------------
DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
ANSIBLE_BASE = os.path.join(DAG_FOLDER, "ansible")
SSH_KEY_PATH = os.path.join(DAG_FOLDER, ".ssh/id_rsa")
ENV_PATH = os.path.join(DAG_FOLDER, ".env")

# --------------------------------------------------
# Step 1: Fetch cluster info from DB
# --------------------------------------------------
def fetch_cluster_info(**context):
    resource_id = context["dag_run"].conf.get("resource_id")
    if not resource_id:
        raise ValueError("resource_id missing")

    load_dotenv(ENV_PATH)
    
    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
    )
    cur = conn.cursor()

    # Get resource config ID and resource name (used as resource group)
    cur.execute('SELECT "resourceConfigId", "name" FROM "Resources" WHERE id = %s;', (resource_id,))
    res = cur.fetchone()
    if not res: raise ValueError("Resource not found")
    resource_config_id, resource_name = res[0], res[1]

    # Get all clusters associated with this config
    cur.execute(
        '''SELECT "id", "clusterName", "clusterEndpoint", "terraformState"
           FROM "AwsK8sCluster" WHERE "resourceConfigId" = %s;''',
        (resource_config_id,)
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows: raise ValueError("No clusters found")

    clusters_data = []

    for cid, name, endpoint, tf_state in rows:
        # Parse JSON
        endpoint = endpoint if isinstance(endpoint, dict) else (json.loads(endpoint) if endpoint else {})
        tf_state = tf_state if isinstance(tf_state, dict) else (json.loads(tf_state) if tf_state else {})

        workers = []
        edge = None

        # Extract resources from Terraform State
        for r in tf_state.get("resources", []):
            if r.get("type") == "aws_instance":
                # Find Edge Node for THIS specific cluster
                if r.get("name") == "k3s_edge":
                    for i in r.get("instances", []):
                        index_key = i.get("index_key")
                        attrs = i.get("attributes", {})
                        tags = attrs.get("tags", {}) or {}

                        if index_key is not None and str(index_key) == str(cid):
                            edge = attrs
                            break

                        if str(tags.get("ClusterId", "")) == str(cid):
                            edge = attrs
                            break

                    # Backward-compatible fallback (older state had a single shared edge)
                    if edge is None and r.get("instances"):
                        edge = r["instances"][0].get("attributes")
                
                # Find Workers belonging to THIS specific cluster (using Tag filtering)
                if r.get("name") == "k3s_worker":
                    for i in r.get("instances", []):
                        # Only add worker if it belongs to this cluster ID
                        if i["attributes"]["tags"].get("ClusterId") == str(cid):
                            workers.append(i["attributes"])

        clusters_data.append({
            "cluster_id": cid,
            "cluster_name": name,
            "resource_group": resource_name,  # Real resource group from database
            "master": endpoint,
            "workers": workers,
            "edge": edge,
        })

    return clusters_data

# --------------------------------------------------
# Step 1.5: Generate Cloudflare Origin Certificates
# --------------------------------------------------
def generate_cloudflare_origin_cert(**context):
    """Generate Cloudflare origin certificate for each cluster"""
    try:
        import requests
    except ImportError:
        print("⚠️ requests module not installed, skipping certificate generation")
        return {}
    
    load_dotenv(ENV_PATH)
    
    CF_API_TOKEN = os.getenv('CLOUDFLARE_API_TOKEN')
    CF_API_KEY = os.getenv('CLOUDFLARE_API_KEY')
    CF_ZONE_ID = os.getenv('CLOUDFLARE_ZONE_ID')
    CF_EMAIL = os.getenv('CLOUDFLARE_EMAIL')
    
    if not CF_ZONE_ID or (not CF_API_TOKEN and not CF_API_KEY):
        print("⚠️ Cloudflare credentials not set, skipping certificate generation")
        print("Set CLOUDFLARE_API_TOKEN (with SSL permissions) or CLOUDFLARE_API_KEY + CLOUDFLARE_EMAIL")
        return {}
    
    clusters = context["ti"].xcom_pull(task_ids="fetch_cluster_info")
    certificates = {}
    
    # Extract unique resource groups to avoid duplicate certificate creation
    resource_groups = list(set(cluster.get('resource_group', 'rg') for cluster in clusters))
    print(f"Found {len(resource_groups)} unique resource groups: {resource_groups}")
    
    # Use API Token (preferred) or Global API Key
    if CF_API_TOKEN:
        headers = {
            'Authorization': f'Bearer {CF_API_TOKEN}',
            'Content-Type': 'application/json'
        }
        print("Using API Token authentication")
    else:
        headers = {
            'X-Auth-Key': CF_API_KEY,
            'X-Auth-Email': CF_EMAIL,
            'Content-Type': 'application/json'
        }
        print("Using Global API Key authentication")
    
    for resource_group in resource_groups:
        # Hostnames for this resource group (covers all clusters in the group)
        hostnames = [
            f"*.{resource_group}.orchestronic.dev",
            f"{resource_group}.orchestronic.dev"
        ]
        
        print(f"Checking certificates for {resource_group}...")
        
        # Check if certificate already exists (using Origin CA API)
        try:
            list_response = requests.get(
                'https://api.cloudflare.com/client/v4/certificates',
                headers=headers,
                params={'zone_id': CF_ZONE_ID},
                timeout=10
            )
            
            existing_cert = None
            if list_response.ok:
                certs = list_response.json().get('result', [])
                for cert in certs:
                    cert_hostnames = cert.get('hostnames', [])
                    # Check if hostnames match
                    if set(hostnames) == set(cert_hostnames):
                        existing_cert = cert
                        print(f"✓ Found existing certificate for {resource_group}")
                        # Note: Cloudflare doesn't return private key after creation
                        print(f"⚠️ Private key not available for existing cert, will create new one")
                        existing_cert = None  # Force recreation to get private key
                        break
            
            if not existing_cert:
                # Create new Origin CA certificate (auto-generate)
                # Generate CSR data
                data = {
                    "hostnames": hostnames,
                    "requested_validity": 5475,  # 15 years in days
                    "request_type": "origin-rsa",
                    "csr": ""  # Empty CSR means Cloudflare generates it
                }
                
                response = requests.post(
                    'https://api.cloudflare.com/client/v4/certificates',  # Origin CA endpoint
                    headers=headers,
                    json=data,
                    timeout=30
                )
                response.raise_for_status()
                
                result = response.json()
                if result.get('success'):
                    cert_data = result['result']
                    certificates[resource_group] = {
                        'certificate': cert_data.get('certificate'),
                        'private_key': cert_data.get('private_key')
                    }
                    print(f"✓ Created certificate for {resource_group}")
                    print(f"  Hostnames: {', '.join(hostnames)}")
                    print(f"  Certificate ID: {cert_data.get('id')}")
                else:
                    print(f"✗ Failed to create certificate: {result.get('errors')}")
        except Exception as e:
            print(f"✗ Error with certificate for {resource_group}: {e}")
    
    return certificates

# --------------------------------------------------
# Step 2: Configure Clusters (Looping Logic)
# --------------------------------------------------
def configure_clusters(**context):
    clusters = context["ti"].xcom_pull(task_ids="fetch_cluster_info")
    certificates = context["ti"].xcom_pull(task_ids="generate_certificates") or {}
    
    # Iterate over every cluster and run Ansible sequentially
    for cluster in clusters:
        print(f"--- Configuring Cluster: {cluster['cluster_name']} ---")
        
        # 1. Generate Unique Inventory for this Cluster
        safe_name = cluster['cluster_name'].replace(" ", "-").lower()
        inventory_path = f"/tmp/hosts_{safe_name}.ini"
        
        edge_public_ip = cluster['edge']['public_ip'] if cluster.get('edge') else None
        proxyjump_arg = (
            f"-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
            f"-o ProxyCommand=\"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {SSH_KEY_PATH} -W %h:%p ubuntu@{edge_public_ip}\""
            if edge_public_ip
            else "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
        )

        master_public_ip = None
        if isinstance(cluster.get('master'), dict):
            master_public_ip = cluster['master'].get('public_ip')

        lines = ["[k3s_master]"]
        lines.append(
            f"master ansible_host={cluster['master']['private_ip']} ansible_user=ubuntu "
            f"ansible_ssh_private_key_file={SSH_KEY_PATH} ansible_ssh_common_args='{proxyjump_arg}' "
            f"k3s_tls_san={master_public_ip or cluster['master']['private_ip']} "
            f"k3s_kubeconfig_server={master_public_ip or cluster['master']['private_ip']}"
        )
        
        lines.append("\n[k3s_workers]")
        for idx, w in enumerate(cluster['workers']):
            lines.append(
                f"worker{idx+1} ansible_host={w['private_ip']} private_ip={w['private_ip']} ansible_user=ubuntu "
                f"ansible_ssh_private_key_file={SSH_KEY_PATH} ansible_ssh_common_args='{proxyjump_arg}'"
            )
        
        lines.append("\n[edge]")
        if cluster['edge']:
            lines.append(f"edge01 ansible_host={cluster['edge']['public_ip']} ansible_user=ubuntu ansible_ssh_private_key_file={SSH_KEY_PATH}")

        # --- IMPORTANT: Variables for Dynamic Routing ---
        resource_group = cluster.get('resource_group', 'rg')  # Use real resource group from database
        lines.append("\n[all:vars]")
        lines.append(f"cluster_name={safe_name}")
        lines.append(f"resource_group={resource_group}")  # Resource group for multi-cluster DNS
        lines.append(f"edge_public_ip={edge_public_ip}")  # For nip.io fallback URLs
        lines.append(f"cluster_domain={safe_name}.{resource_group}.orchestronic.dev")  # Full domain with resource group
        
        # Add SSL certificates if available (keyed by resource_group, not cluster name)
        if resource_group in certificates:
            cert_data = certificates[resource_group]
            if cert_data.get('certificate') and cert_data.get('private_key'):
                # Escape newlines for Ansible variable
                cert_content = cert_data['certificate'].replace('\n', '\\n')
                key_content = cert_data['private_key'].replace('\n', '\\n')
                lines.append(f'cloudflare_origin_cert="{cert_content}"')
                lines.append(f'cloudflare_origin_key="{key_content}"')
                print(f"✓ SSL certificates added to inventory for {resource_group}")
        
        with open(inventory_path, "w") as f:
            f.write("\n".join(lines))
        
        print(f"Generated inventory: {inventory_path}")
        print("Inventory content:\n" + "\n".join(lines))

        # 2. Run Ansible Playbooks using Subprocess
        env = os.environ.copy()
        env["ANSIBLE_HOST_KEY_CHECKING"] = "False"
        env["ANSIBLE_ROLES_PATH"] = os.path.join(ANSIBLE_BASE, "roles")
        env["ANSIBLE_PRIVATE_KEY_FILE"] = SSH_KEY_PATH

        playbooks = ["master.yml", "worker.yml", "edge.yml"]
        
        for pb in playbooks:
            pb_path = os.path.join(ANSIBLE_BASE, "playbooks", pb)
            cmd = ["ansible-playbook", "-vvv", "-i", inventory_path, pb_path]
            
            print(f"Running playbook: {pb} for {cluster['cluster_name']}")
            result = subprocess.run(cmd, env=env, cwd=ANSIBLE_BASE, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"ERROR in {pb}")
                print(f"Return code: {result.returncode}")
                print(f"Command: {' '.join(cmd)}")
                if result.stdout:
                    print(f"STDOUT:\n{result.stdout}")
                if result.stderr:
                    print(f"STDERR:\n{result.stderr}")
                raise RuntimeError(f"Ansible failed for {cluster['cluster_name']}")
            else:
                print(f"SUCCESS: {pb}")

    print("All clusters configured successfully.")

# --------------------------------------------------
# Step 2.5: Create Cloudflare DNS Records
# --------------------------------------------------
def create_cloudflare_dns(**context):
    """Create wildcard DNS for each cluster's edge VM"""
    try:
        import requests
    except ImportError:
        print("⚠️ requests module not installed, skipping DNS creation")
        return
    
    load_dotenv(ENV_PATH)
    
    CF_API_TOKEN = os.getenv('CLOUDFLARE_API_TOKEN')
    CF_API_KEY = os.getenv('CLOUDFLARE_API_KEY')
    CF_ZONE_ID = os.getenv('CLOUDFLARE_ZONE_ID')
    CF_EMAIL = os.getenv('CLOUDFLARE_EMAIL')
    
    if not CF_ZONE_ID or (not CF_API_TOKEN and not CF_API_KEY):
        print("⚠️ Cloudflare credentials not set, skipping DNS creation")
        print("Set CLOUDFLARE_API_TOKEN or CLOUDFLARE_API_KEY + CLOUDFLARE_EMAIL")
        return
    
    # Use API Token (preferred) or Global API Key
    if CF_API_TOKEN:
        headers = {
            'Authorization': f'Bearer {CF_API_TOKEN}',
            'Content-Type': 'application/json'
        }
    else:
        headers = {
            'X-Auth-Key': CF_API_KEY,
            'X-Auth-Email': CF_EMAIL,
            'Content-Type': 'application/json'
        }
    
    clusters = context["ti"].xcom_pull(task_ids="fetch_cluster_info")
    
    # Group clusters by resource group and get edge IP for each
    rg_to_edge = {}
    for cluster in clusters:
        resource_group = cluster.get('resource_group', 'rg')
        edge_ip = cluster.get('edge', {}).get('public_ip')
        if edge_ip and resource_group not in rg_to_edge:
            rg_to_edge[resource_group] = edge_ip
    
    print(f"Creating DNS for {len(rg_to_edge)} unique resource groups")
    
    for resource_group, edge_ip in rg_to_edge.items():
        record_name = f"*.{resource_group}"  # Matches URL pattern: app.resourcegroup.orchestronic.dev
        full_domain = f"{record_name}.orchestronic.dev"
        
        data = {
            "type": "A",
            "name": record_name,
            "content": edge_ip,
            "ttl": 120,
            "proxied": True  # Enable Cloudflare proxy for SSL
        }
        
        try:
            # Check if record exists
            response = requests.get(
                f'https://api.cloudflare.com/client/v4/zones/{CF_ZONE_ID}/dns_records?name={full_domain}',
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            existing = response.json().get('result', [])
            
            if existing:
                # Update existing record
                record_id = existing[0]['id']
                response = requests.put(
                    f'https://api.cloudflare.com/client/v4/zones/{CF_ZONE_ID}/dns_records/{record_id}',
                    headers=headers,
                    json=data,
                    timeout=10
                )
                response.raise_for_status()
                print(f"✓ Updated DNS: {full_domain} → {edge_ip}")
            else:
                # Create new record
                response = requests.post(
                    f'https://api.cloudflare.com/client/v4/zones/{CF_ZONE_ID}/dns_records',
                    headers=headers,
                    json=data,
                    timeout=10
                )
                response.raise_for_status()
                print(f"✓ Created DNS: {full_domain} → {edge_ip}")
        except Exception as e:
            print(f"✗ Failed to create DNS for {full_domain}: {e}")

# --------------------------------------------------
# Step 3: Fetch Kubeconfigs (Looping Logic)
# --------------------------------------------------
def fetch_kubeconfigs(**context):
    clusters = context["ti"].xcom_pull(task_ids="fetch_cluster_info")
    load_dotenv(ENV_PATH)
    
    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
    )
    cur = conn.cursor()

    for cluster in clusters:
        edge_public_ip = cluster["edge"]["public_ip"] if cluster.get("edge") else None
        master_ip = cluster["master"]["private_ip"]
        cluster_id = cluster["cluster_id"]
        safe_name = cluster['cluster_name'].replace(" ", "-").lower()
        temp_kube_path = f"/tmp/kubeconfig_{safe_name}"

        if not edge_public_ip:
            print(f"No edge node found for {cluster['cluster_name']}; cannot fetch kubeconfig from private master")
            continue

        proxy_cmd = (
            f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {SSH_KEY_PATH} "
            f"-W %h:%p ubuntu@{edge_public_ip}"
        )
        cmd = (
            f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {SSH_KEY_PATH} "
            f"-o ProxyCommand=\"{proxy_cmd}\" "
            f"ubuntu@{master_ip} 'cat /home/ubuntu/.kube/config' > {temp_kube_path}"
        )
        
        if os.system(cmd) != 0:
            print(f"Failed to fetch kubeconfig for {cluster['cluster_name']}")
            continue

        with open(temp_kube_path) as f:
            kubeconfig = f.read()

        if kubeconfig.strip():
            cur.execute(
                'UPDATE "AwsK8sCluster" SET "kubeConfig" = %s WHERE id = %s;',
                (kubeconfig, cluster_id),
            )
            conn.commit()
            print(f"Updated DB for {cluster['cluster_name']}")
            
        if os.path.exists(temp_kube_path):
            os.remove(temp_kube_path)

    cur.close()
    conn.close()

# --------------------------------------------------
# DAG Definition
# --------------------------------------------------
with DAG(
    dag_id="AWS_configure_k3s",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Configure K3s clusters dynamically with SSL",
) as dag:

    fetch_info = PythonOperator(
        task_id="fetch_cluster_info",
        python_callable=fetch_cluster_info,
    )

    generate_certs = PythonOperator(
        task_id="generate_certificates",
        python_callable=generate_cloudflare_origin_cert,
    )

    configure_all = PythonOperator(
        task_id="configure_clusters",
        python_callable=configure_clusters,
    )

    create_dns = PythonOperator(
        task_id="create_cloudflare_dns",
        python_callable=create_cloudflare_dns,
    )

    store_configs = PythonOperator(
        task_id="store_kubeconfigs",
        python_callable=fetch_kubeconfigs,
    )

    fetch_info >> generate_certs >> configure_all >> create_dns >> store_configs