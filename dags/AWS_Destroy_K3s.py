import os
import json
import pika
import psycopg2
import shutil
import ast
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from dotenv import load_dotenv
from os.path import expanduser

load_dotenv(expanduser('/opt/airflow/dags/.env'))

# -------------------------
# Default DAG args
# -------------------------
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}


def _parse_config(config):
    if isinstance(config, dict):
        return config
    if isinstance(config, str):
        try:
            return json.loads(config)
        except Exception:
            return ast.literal_eval(config)
    raise TypeError(f"Unsupported config type: {type(config)}")


def _pick_rg_terraform_dir(repo_name: str) -> str:
    """Pick the best RG terraform directory.

    Preferred: /opt/airflow/dags/terraform/<repo>/aws/rg
    Legacy:    /opt/airflow/dags/terraform/<repo>/rg
    """
    preferred = f"/opt/airflow/dags/terraform/{repo_name}/aws/rg"
    legacy = f"/opt/airflow/dags/terraform/{repo_name}/rg"

    preferred_state = os.path.join(preferred, "terraform.tfstate")
    legacy_state = os.path.join(legacy, "terraform.tfstate")

    if os.path.exists(preferred_state):
        return preferred
    if os.path.exists(legacy_state):
        print(f"[!] Using legacy RG terraform dir (state found): {legacy}")
        return legacy

    # Default to preferred path so the directory structure stays consistent.
    return preferred

# -------------------------
# DB Connection Helper
# -------------------------
def get_db_connection():
    load_dotenv(expanduser('/opt/airflow/dags/.env'))
    return psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
    )

# -------------------------
# Step 1: RabbitMQ consumer
# -------------------------
def rabbitmq_consumer():
    load_dotenv(expanduser('/opt/airflow/dags/.env'))
    rabbit_url = os.getenv("RABBITMQ_URL")
    if not rabbit_url:
        raise ValueError("RABBITMQ_URL is not set in .env")

    connection = pika.BlockingConnection(pika.URLParameters(rabbit_url))
    channel = connection.channel()

    # Consumes from 'destroyK8s' queue
    method_frame, header_frame, body = channel.basic_get(queue='destroyK8s', auto_ack=True)
    if method_frame:
        message = body.decode()
        obj = json.loads(message)
        resource_id = obj["data"]["resourceId"]
        print(f"[x] Got message to destroy resource: {resource_id}")
        connection.close()
        return resource_id
    else:
        print("[x] No message in destroyK8s queue")
        connection.close()
        return None

# -------------------------
# Step 2: Get repository name (Project Name)
# -------------------------
def get_repository_name(resource_id):
    if not resource_id:
        raise ValueError("No resource ID provided")
        
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute('SELECT "name" FROM "Resources" WHERE id = %s;', (resource_id,))
        res = cursor.fetchone()
        if not res:
            # If resource is missing from DB, we still need the name to clean up AWS.
            # This is a critical failure point if the DB is desynced.
            # For now, we raise, but in production, you might want to pass the repoName in the RabbitMQ payload.
            raise ValueError(f"No resource found for id={resource_id}. Cannot determine Terraform path.")
        return res[0]
    finally:
        cursor.close()
        connection.close()


# -------------------------
# Step 2b: Fetch full destroy config
# -------------------------
def fetch_destroy_config(resource_id):
    if not resource_id:
        raise ValueError("No resource ID provided")

    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(
            'SELECT "name", "region", "resourceConfigId" FROM "Resources" WHERE id = %s;',
            (resource_id,),
        )
        res = cursor.fetchone()
        if not res:
            raise ValueError(f"No resource found for id={resource_id}")
        repo_name, region, resource_config_id = res

        cursor.execute(
            'SELECT "id", "clusterName", "nodeCount", "nodeSize", "terraformState" '
            'FROM "AwsK8sCluster" WHERE "resourceConfigId" = %s;',
            (resource_config_id,),
        )
        rows = cursor.fetchall()
        if not rows:
            raise ValueError(f"No AwsK8sCluster rows found for resourceConfigId={resource_config_id}")

        clusters = []
        k3s_state = None
        for cid, cname, node_count, node_size, tf_state in rows:
            clusters.append(
                {
                    "id": str(cid),
                    "cluster_name": cname,
                    "node_count": int(node_count),
                    "node_size": node_size,
                }
            )
            if k3s_state is None and tf_state:
                k3s_state = tf_state if isinstance(tf_state, dict) else json.loads(tf_state)

        return {
            "resource_id": resource_id,
            "repo_name": repo_name,
            "region": region,
            "resource_config_id": str(resource_config_id),
            "project_name": f"{repo_name}-{str(resource_id)[:4]}",
            "k3s_clusters": clusters,
            "k3s_terraform_state": k3s_state,
        }
    finally:
        cursor.close()
        connection.close()


# -------------------------
# Step 3: Recreate Terraform dirs/files
# -------------------------
def prepare_k3s_terraform_files(config):
    config = _parse_config(config)

    repo_name = config["repo_name"]
    terraform_dir = f"/opt/airflow/dags/terraform/{repo_name}/k3s"
    os.makedirs(terraform_dir, exist_ok=True)

    # Re-generate Terraform configuration (same as provision DAG)
    # Import inside function to avoid parse-time side effects in Airflow.
    from AWS_provide_k3s import write_terraform_files  # type: ignore

    config_info = {
        "resourcesId": config["resource_id"],
        "repoName": repo_name,
        "region": config["region"],
        "k3s_clusters": config["k3s_clusters"],
    }
    write_terraform_files(terraform_dir, config_info)

    # Restore terraform.tfstate from DB so destroy works even if the folder was deleted.
    state = config.get("k3s_terraform_state")
    if not state:
        raise ValueError(
            "Missing k3s terraformState in DB; cannot safely destroy compute resources. "
            "Provision must store terraformState in AwsK8sCluster."
        )

    state_path = os.path.join(terraform_dir, "terraform.tfstate")
    with open(state_path, "w") as f:
        json.dump(state, f)

    return terraform_dir


def prepare_rg_terraform_files(config):
    config = _parse_config(config)

    repo_name = config["repo_name"]
    terraform_dir = _pick_rg_terraform_dir(repo_name)
    os.makedirs(terraform_dir, exist_ok=True)

    # Re-generate Terraform configuration (same as network provision DAG)
    from AWS_Resources_Cluster import write_terraform_files as write_rg_files  # type: ignore

    config_info = json.dumps(
        {
            "resourceId": config["resource_id"],
            "repoName": repo_name,
            "region": config["region"],
            "cloudProvider": "aws",
            "k8sCount": len(config.get("k3s_clusters", [])),
        }
    )
    write_rg_files(terraform_dir, config_info)

    # NOTE: This DAG (today) does not persist RG terraform state in DB.
    # If terraform.tfstate is missing here, we fail loudly to avoid silently orphaning VPC resources.
    state_path = os.path.join(terraform_dir, "terraform.tfstate")
    if not os.path.exists(state_path):
        # Try to recover state from the other known path if the directory layout changed.
        preferred = f"/opt/airflow/dags/terraform/{repo_name}/aws/rg"
        legacy = f"/opt/airflow/dags/terraform/{repo_name}/rg"
        candidates = [preferred, legacy]
        for candidate in candidates:
            candidate_state = os.path.join(candidate, "terraform.tfstate")
            if candidate != terraform_dir and os.path.exists(candidate_state):
                shutil.copy2(candidate_state, state_path)
                print(f"[!] Recovered RG terraform state from {candidate_state} -> {state_path}")
                break

    if not os.path.exists(state_path):
        raise FileNotFoundError(
            f"Missing {state_path}. Cannot destroy network without Terraform state. "
            "Terraform destroy is what deletes VPC/subnets/ENIs/EIPs/IGW/route tables/SGs. "
            "Ensure the RG terraform directory (and terraform.tfstate) was not deleted before running destroy."
        )

    return terraform_dir

# -------------------------
# Step 3: Cleanup directories
# -------------------------
def cleanup_directories(repoName):
    # Allow passing the full destroy config object
    if isinstance(repoName, dict):
        repoName = repoName.get("repo_name")
    elif isinstance(repoName, str) and repoName.strip().startswith("{"):
        try:
            repoName = json.loads(repoName).get("repo_name")
        except Exception:
            repoName = ast.literal_eval(repoName).get("repo_name")

    if not repoName:
        raise ValueError("Missing repo_name for cleanup")

    base_path = f"/opt/airflow/dags/terraform/{repoName}"

    # Support both current and legacy layouts
    dirs_to_remove = [
        os.path.join(base_path, "k3s"),
        os.path.join(base_path, "aws", "rg"),
        os.path.join(base_path, "rg"),
        os.path.join(base_path, "aws"),
    ]

    for path in dirs_to_remove:
        if os.path.exists(path):
            shutil.rmtree(path)
            print(f"Removed directory: {path}")

    # Clean up parent if empty
    if os.path.exists(base_path) and not os.listdir(base_path):
        os.rmdir(base_path)
        print(f"Removed empty parent directory: {base_path}")
        
    return repoName

# -------------------------
# Step 4: Delete database records
# -------------------------
def supabase_delete_resource(resource_id):
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        # Get resourceConfigId before deleting the Resource
        cursor.execute('SELECT "resourceConfigId" FROM "Resources" WHERE id = %s;', (resource_id,))
        res = cursor.fetchone()
        
        resourceConfigId = res[0] if res else None

        # 1. Delete K3s clusters (Child records)
        if resourceConfigId:
            cursor.execute('DELETE FROM "AwsK8sCluster" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
            print(f"Deleted AwsK8sCluster records for config {resourceConfigId}")

        # 2. Delete Resource (Parent of K3s, Child of Config)
        cursor.execute('DELETE FROM "Resources" WHERE id = %s;', (resource_id,))
        print(f"Deleted Resources record {resource_id}")

        # 3. Delete ResourceConfig (Root configuration)
        if resourceConfigId:
            cursor.execute('DELETE FROM "ResourceConfig" WHERE id = %s;', (resourceConfigId,))
            print(f"Deleted ResourceConfig {resourceConfigId}")
            
        connection.commit()

    except Exception as e:
        connection.rollback()
        raise e
    finally:
        cursor.close()
        connection.close()

# -------------------------
# DAG Definition
# -------------------------
with DAG(
    dag_id='AWS_Destroy_K3s',
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Receive Message
    get_resource_id = PythonOperator(
        task_id="get_resource_id",
        python_callable=rabbitmq_consumer,
    )

    # 2. Identify Project/Repo
    fetch_destroy_config_task = PythonOperator(
        task_id="fetch_destroy_config",
        python_callable=fetch_destroy_config,
        op_args=["{{ ti.xcom_pull(task_ids='get_resource_id') }}"],
    )

    prepare_k3s_tf = PythonOperator(
        task_id="prepare_k3s_terraform",
        python_callable=prepare_k3s_terraform_files,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_destroy_config') | tojson }}"],
    )

    # 3. Destroy Compute Layer (K3s VMs, SGs, IAM, KeyPairs)
    # DEPENDENCY: Must run BEFORE network destroy.
    # LOGIC: Checks if the directory exists. If yes, Terraform Destroy. If no, skip silently.
    destroy_k3s = BashOperator(
        task_id="terraform_destroy_k3s",
        bash_command=(
            'TF_DIR="{{ ti.xcom_pull(task_ids=\'prepare_k3s_terraform\') }}"; '
            'echo "Using K3s Terraform dir: $TF_DIR"; '
            'cd "$TF_DIR" && terraform init -input=false -lock-timeout=5m '
            '&& terraform destroy -auto-approve -input=false -refresh=true -lock-timeout=5m'
        ),
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    prepare_rg_tf = PythonOperator(
        task_id="prepare_rg_terraform",
        python_callable=prepare_rg_terraform_files,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_destroy_config') | tojson }}"],
        trigger_rule='all_success',
    )

    # 4. Destroy Network Layer (VPC, Subnets, IGW)
    # DEPENDENCY: Runs only if `destroy_k3s` succeeded (or skipped successfully).
    destroy_rg = BashOperator(
        task_id="terraform_destroy_rg",
        bash_command=(
            'TF_DIR="{{ ti.xcom_pull(task_ids=\'prepare_rg_terraform\') }}"; '
            'echo "Using RG Terraform dir: $TF_DIR"; '
            'cd "$TF_DIR" && terraform init -input=false -lock-timeout=5m '
            '&& terraform destroy -auto-approve -input=false -refresh=true -lock-timeout=5m'
        ),
        trigger_rule='all_success',
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    # 5. Cleanup File System
    cleanup_fs = PythonOperator(
        task_id="cleanup_directories",
        python_callable=cleanup_directories,
        op_args=["{{ ti.xcom_pull(task_ids='fetch_destroy_config') | tojson }}"],
        trigger_rule='all_done', # Run cleanup even if terraform reported issues, to try and clear temp files
    )

    # 6. Delete DB Records
    delete_db_records = PythonOperator(
        task_id='supabase_delete_resource',
        python_callable=supabase_delete_resource,
        op_args=["{{ ti.xcom_pull(task_ids='get_resource_id') }}"],
        trigger_rule='all_success',
    )

    end = EmptyOperator(task_id="end")

    # -------------------------
    # Workflow Logic
    # -------------------------
    
    get_resource_id >> fetch_destroy_config_task >> prepare_k3s_tf
    
    # Linear execution ensures dependencies are respected (Compute -> Network)
    prepare_k3s_tf >> destroy_k3s >> prepare_rg_tf >> destroy_rg 
    
    destroy_rg >> cleanup_fs >> delete_db_records >> end