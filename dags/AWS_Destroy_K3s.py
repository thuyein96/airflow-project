import os
import json
import pika
import psycopg2
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
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
        # Matches logic in AWS_Resources_Cluster.py
        cursor.execute('SELECT "name" FROM "Resources" WHERE id = %s;', (resource_id,))
        res = cursor.fetchone()
        if not res:
            raise ValueError(f"No resource found for id={resource_id}")
        return res[0]
    finally:
        cursor.close()
        connection.close()

# -------------------------
# Step 3: Check if K3s clusters exist (Branching)
# -------------------------
def check_k3s_clusters(resource_id):
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        # Get resourceConfigId
        cursor.execute('SELECT "resourceConfigId" FROM "Resources" WHERE id = %s;', (resource_id,))
        res = cursor.fetchone()
        if not res:
            print(f"Resource {resource_id} not found, checking if cleanup is still possible...")
            return 'skip_destroy_k3s'
        
        resourceConfigId = res[0]

        # Check for K3s clusters
        cursor.execute('SELECT id FROM "AwsK8sCluster" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
        k3s_instances = cursor.fetchall()
        
        if len(k3s_instances) > 0:
            return 'terraform_destroy_k3s'
        else:
            return 'skip_destroy_k3s'
    finally:
        cursor.close()
        connection.close()

# -------------------------
# Step 6: Cleanup directories
# -------------------------
def cleanup_directories(repoName):
    base_path = f"/opt/airflow/dags/terraform/{repoName}"
    k3s_path = os.path.join(base_path, "k3s")
    rg_path = os.path.join(base_path, "rg")

    # Remove K3s dir
    if os.path.exists(k3s_path):
        shutil.rmtree(k3s_path)
        print(f"Removed directory: {k3s_path}")
    
    # Remove RG dir
    if os.path.exists(rg_path):
        shutil.rmtree(rg_path)
        print(f"Removed directory: {rg_path}")

    # Clean up parent if empty
    if os.path.exists(base_path) and not os.listdir(base_path):
        os.rmdir(base_path)
        print(f"Removed empty parent directory: {base_path}")
        
    return repoName

# -------------------------
# Step 7: Delete database records
# -------------------------
def supabase_delete_resource(resource_id):
    connection = get_db_connection()
    cursor = connection.cursor()
    try:
        # Get resourceConfigId
        cursor.execute('SELECT "resourceConfigId" FROM "Resources" WHERE id = %s;', (resource_id,))
        res = cursor.fetchone()
        
        if res:
            resourceConfigId = res[0]

            # 1. Delete K3s clusters (Child records)
            cursor.execute('DELETE FROM "AwsK8sCluster" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
            print(f"Deleted AwsK8sCluster records for config {resourceConfigId}")

            # 2. Delete Resource (Parent of K3s, Child of Config)
            cursor.execute('DELETE FROM "Resources" WHERE id = %s;', (resource_id,))
            print(f"Deleted Resources record {resource_id}")

            # 3. Delete ResourceConfig (Root configuration)
            cursor.execute('DELETE FROM "ResourceConfig" WHERE id = %s;', (resourceConfigId,))
            print(f"Deleted ResourceConfig {resourceConfigId}")
            
            connection.commit()
        else:
            print(f"Resource {resource_id} not found in DB, assuming already deleted.")

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
    get_repository_name_task = PythonOperator(
        task_id="get_repository_name",
        python_callable=get_repository_name,
        op_args=["{{ ti.xcom_pull(task_ids='get_resource_id') }}"],
    )

    # 3. Branch: Do we have K3s clusters to destroy?
    check_clusters = BranchPythonOperator(
        task_id='check_k3s_clusters',
        python_callable=check_k3s_clusters,
        op_args=["{{ ti.xcom_pull(task_ids='get_resource_id') }}"],
    )

    skip_destroy_k3s = EmptyOperator(task_id="skip_destroy_k3s")

    # 4. Destroy Compute Layer (K3s VMs, SGs, IAM)
    # MUST run before Network layer because VMs depend on VPC/Subnets
    destroy_k3s = BashOperator(
        task_id="terraform_destroy_k3s",
        bash_command=(
            'TF_DIR="/opt/airflow/dags/terraform/{{ ti.xcom_pull(task_ids=\'get_repository_name\') }}/k3s"; '
            'if [ -d "$TF_DIR" ]; then '
            '   cd "$TF_DIR" && terraform init -input=false && terraform destroy -auto-approve -input=false; '
            'else '
            '   echo "[x] Directory $TF_DIR does not exist, skipping K3s destroy."; '
            'fi'
        ),
        retries=3,
        retry_delay=timedelta(minutes=2)
    )

    # 5. Destroy Network Layer (VPC, Subnets, IGW)
    destroy_rg = BashOperator(
        task_id="terraform_destroy_rg",
        bash_command=(
            'TF_DIR="/opt/airflow/dags/terraform/{{ ti.xcom_pull(task_ids=\'get_repository_name\') }}/rg"; '
            'if [ -d "$TF_DIR" ]; then '
            '   cd "$TF_DIR" && terraform init -input=false && terraform destroy -auto-approve -input=false; '
            'else '
            '   echo "[x] Directory $TF_DIR does not exist, skipping RG destroy."; '
            'fi'
        ),
        trigger_rule='all_success', # Only run if K3s destroy (or skip) succeeded
        retries=3,
        retry_delay=timedelta(minutes=2)
    )

    # 6. Cleanup File System
    cleanup_fs = PythonOperator(
        task_id="cleanup_directories",
        python_callable=cleanup_directories,
        op_args=["{{ ti.xcom_pull(task_ids='get_repository_name') }}"],
        trigger_rule='all_done', # Run even if terraform failed slightly, to ensure we try to clean up
    )

    # 7. Delete DB Records
    delete_db_records = PythonOperator(
        task_id='supabase_delete_resource',
        python_callable=supabase_delete_resource,
        op_args=["{{ ti.xcom_pull(task_ids='get_resource_id') }}"],
        trigger_rule='all_success', # Only delete DB if FS cleanup didn't crash hard
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    end = EmptyOperator(task_id="end")

    # Flow
    get_resource_id >> get_repository_name_task >> check_clusters
    
    check_clusters >> destroy_k3s
    check_clusters >> skip_destroy_k3s
    
    # Network destroy runs after Compute destroy (or skip)
    destroy_k3s >> destroy_rg
    skip_destroy_k3s >> destroy_rg
    
    destroy_rg >> cleanup_fs >> delete_db_records >> end