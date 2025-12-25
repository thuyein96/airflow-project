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
# Step 1: RabbitMQ consumer
# -------------------------
def rabbitmq_consumer():
    load_dotenv(expanduser('/opt/airflow/dags/.env'))
    rabbit_url = os.getenv("RABBITMQ_URL")
    if not rabbit_url:
        raise ValueError("RABBITMQ_URL is not set in .env")

    connection = pika.BlockingConnection(pika.URLParameters(rabbit_url))
    channel = connection.channel()

    # Queue name kept as-is (current producer uses destroyK8s)
    method_frame, header_frame, body = channel.basic_get(queue='destroyK8s', auto_ack=True)
    if method_frame:
        message = body.decode()
        obj = json.loads(message)
        resource_id = obj["data"]["resourceId"]
        print(f"[x] Got message: {resource_id}")
        connection.close()
        return resource_id
    else:
        print("[x] No message in destroyK8s queue")
        connection.close()
        return None

# -------------------------
# Step 2: Get repository/project name
# -------------------------
def repository_name(resource_id):
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
    try:
        cursor.execute('SELECT "name" FROM "Resources" WHERE id = %s;', (resource_id,))
        res = cursor.fetchone()
        if not res:
            raise ValueError(f"No resource found for id={resource_id}")
        return res[0]
    finally:
        cursor.close()
        connection.close()

# -------------------------
# Step 3: Check if K3s clusters exist
# -------------------------
def check_k3s_clusters(resource_id):
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
    try:
        # Get resourceConfigId
        cursor.execute(
            'SELECT "resourceConfigId" FROM "Resources" WHERE id = %s;',
            (resource_id,)
        )
        res = cursor.fetchone()
        if not res:
            raise ValueError(f"No resource found for id={resource_id}")
        resourceConfigId = res[0]

        # Check for K3s clusters
        cursor.execute(
            'SELECT id FROM "AwsK8sCluster" WHERE "resourceConfigId" = %s;',
            (resourceConfigId,)
        )
        k3s_instances = cursor.fetchall()
        k3s_count = len(k3s_instances)

        cursor.close()
        connection.close()

        if k3s_count > 0:
            return 'terraform_destroy_k3s'
        else:
            return 'skip_destroy'
    finally:
        cursor.close()
        connection.close()

# -------------------------
# Step 4: Cleanup folder
# -------------------------
def cleanup_directory(projectName):
    directory_path = f"/opt/airflow/dags/terraform/{projectName}/k3s"
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
    return projectName

def cleanup_rg_directory(projectName):
    directory_path = f"/opt/airflow/dags/terraform/{projectName}/rg"
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
    return projectName

# -------------------------
# Step 5: Delete database records
# -------------------------
def supabase_delete_resource(resource_id):
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
    try:
        # Get resourceConfigId
        cursor.execute(
            'SELECT "resourceConfigId" FROM "Resources" WHERE id = %s;',
            (resource_id,)
        )
        res = cursor.fetchone()
        if not res:
            raise ValueError(f"No resource found for id={resource_id}")
        resourceConfigId = res[0]

        # Delete K3s clusters
        cursor.execute(
            'DELETE FROM "AwsK8sCluster" WHERE "resourceConfigId" = %s;',
            (resourceConfigId,)
        )
        connection.commit()
        print(f"Deleted AwsK8sCluster with resourceConfigId={resourceConfigId}")

        # Delete resource
        cursor.execute(
            'DELETE FROM "Resources" WHERE id = %s;',
            (resource_id,)
        )
        connection.commit()
        print(f"Deleted Resources with id={resource_id}")

        # Delete ResourceConfig
        cursor.execute(
            'DELETE FROM "ResourceConfig" WHERE id = %s;',
            (resourceConfigId,)
        )
        connection.commit()
        print(f"Deleted ResourceConfig with id={resourceConfigId}")

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

    # Step 1: Get resource ID from RabbitMQ
    get_resource_id = PythonOperator(
        task_id="get_resource_id",
        python_callable=rabbitmq_consumer,
    )

    # Step 2: Get repository/project name
    get_repository_name = PythonOperator(
        task_id="get_repository_name",
        python_callable=repository_name,
        op_args=["{{ ti.xcom_pull(task_ids='get_resource_id') }}"],
    )

    # Step 3: Check if K3s clusters exist
    branch_task = BranchPythonOperator(
        task_id='check_k3s_clusters',
        python_callable=check_k3s_clusters,
        op_args=["{{ ti.xcom_pull(task_ids='get_resource_id') }}"],
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    skip_destroy = EmptyOperator(task_id="skip_destroy")

    # Step 4: Destroy K3s infrastructure
    destroy_k3s = BashOperator(
        task_id="terraform_destroy_k3s",
        bash_command=(
            'TF_DIR="/opt/airflow/dags/terraform/{{ ti.xcom_pull(task_ids=\'get_repository_name\') | trim | replace(\'"\',\'\') }}/k3s"; '
            'if [ -d "$TF_DIR" ]; then cd "$TF_DIR" && terraform init -input=false && terraform destroy -auto-approve -input=false; '
            'else echo "[x] No k3s terraform dir: $TF_DIR"; fi'
        ),
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    # Destroy shared VPC (created by AWS_Resources_Cluster in /rg)
    destroy_rg = BashOperator(
        task_id="terraform_destroy_rg",
        bash_command=(
            'TF_DIR="/opt/airflow/dags/terraform/{{ ti.xcom_pull(task_ids=\'get_repository_name\') | trim | replace(\'"\',\'\') }}/rg"; '
            'if [ -d "$TF_DIR" ]; then cd "$TF_DIR" && terraform init -input=false && terraform destroy -auto-approve -input=false; '
            'else echo "[x] No rg terraform dir: $TF_DIR"; fi'
        ),
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    # Step 5: Cleanup folder
    cleanup_dir = PythonOperator(
        task_id="cleanup_dir",
        python_callable=cleanup_directory,
        op_args=["{{ ti.xcom_pull(task_ids='get_repository_name') }}"],
        trigger_rule='all_done',
    )

    cleanup_rg_dir = PythonOperator(
        task_id="cleanup_rg_dir",
        python_callable=cleanup_rg_directory,
        op_args=["{{ ti.xcom_pull(task_ids='get_repository_name') }}"],
        trigger_rule='all_done',
    )

    # Step 6: Delete database records
    delete_resource = PythonOperator(
        task_id='supabase_delete_resource',
        python_callable=supabase_delete_resource,
        op_args=["{{ ti.xcom_pull(task_ids='get_resource_id') }}"],
        trigger_rule='all_done',
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    end = EmptyOperator(task_id="end")

    get_resource_id >> get_repository_name >> branch_task
    branch_task >> destroy_k3s
    branch_task >> skip_destroy

    # Always attempt to destroy shared networking after either branch
    destroy_k3s >> destroy_rg
    skip_destroy >> destroy_rg

    destroy_rg >> cleanup_dir >> cleanup_rg_dir >> delete_resource >> end
