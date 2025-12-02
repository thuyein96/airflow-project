import os
import json
import pika
import psycopg2
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
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

    method_frame, header_frame, body = channel.basic_get(queue='destroy', auto_ack=True)
    if method_frame:
        message = body.decode()
        obj = json.loads(message)
        request_id = obj["data"]["requestId"]
        print(f"[x] Got message: {request_id}")
        connection.close()
        return request_id
    else:
        print("[x] No message in queue")
        connection.close()
        return None

# -------------------------
# Step 2: Get repository/project name
# -------------------------
def repository_name(request_id):
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
        cursor.execute('SELECT "repositoryId" FROM "Request" WHERE id = %s;', (request_id,))
        res = cursor.fetchone()
        if not res:
            raise ValueError(f"No request found for id={request_id}")
        repositoryId = res[0]

        cursor.execute('SELECT "name" FROM "Repository" WHERE id = %s;', (repositoryId,))
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No repository found for id={repositoryId}")
        return row[0]
    finally:
        cursor.close()
        connection.close()

# -------------------------
# Step 3: Branch based on resources
# -------------------------
def branch_resources(request_id):
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
        cursor.execute('SELECT "resourcesId" FROM "Request" WHERE id = %s;', (request_id,))
        res = cursor.fetchone()
        if not res:
            raise ValueError(f"No request found for id={request_id}")
        resourcesId = res[0]

        cursor.execute(
            'SELECT "resourceConfigId" FROM "Resources" WHERE id = %s;',
            (resourcesId,)
        )
        resource = cursor.fetchone()
        if not resource:
            raise ValueError(f"No resource found for resourcesId={resourcesId}")

        resourceConfigId = resource[0]
        
        # Count Azure VM instances
        cursor.execute(
            'SELECT id FROM "AzureVMInstance" WHERE "resourceConfigId" = %s;',
            (resourceConfigId,)
        )
        vm_instances = cursor.fetchall()
        vm_count = len(vm_instances)

        # Count Azure DB instances
        cursor.execute(
            'SELECT id FROM "AzureDatabaseInstance" WHERE "resourceConfigId" = %s;',
            (resourceConfigId,)
        )
        db_instances = cursor.fetchall()
        db_count = len(db_instances)

        # Count Azure Storage instances
        cursor.execute(
            'SELECT id FROM "AzureStorageInstance" WHERE "resourceConfigId" = %s;',
            (resourceConfigId,)
        )
        st_instances = cursor.fetchall()
        st_count = len(st_instances)

        # Count Azure K8s clusters
        cursor.execute(
            'SELECT id FROM "AzureK8sCluster" WHERE "resourceConfigId" = %s;',
            (resourceConfigId,)
        )
        k8s_instances = cursor.fetchall()
        k8s_count = len(k8s_instances)

        branches = []
        if vm_count > 0:
            branches.append('terraform_destroy_vm')
        if db_count > 0:
            branches.append('terraform_destroy_db')
        if st_count > 0:
            branches.append('terraform_destroy_st')
        if k8s_count > 0:
            branches.append('terraform_destroy_k8s')
        
        # Always destroy resource group last
        # branches.append('terraform_destroy_rg')
        
        if not branches:
            return 'end'
        
        return branches
    finally:
        cursor.close()
        connection.close()

# -------------------------
# Step 4: Cleanup folder
# -------------------------
def cleanup_directory(projectName):
    directory_path = f"/opt/airflow/dags/terraform/{projectName}"
    if os.path.exists(directory_path):
        shutil.rmtree(directory_path)
    return projectName

# -------------------------
# Step 5: Delete from Database
# -------------------------
def supabase_delete_request(request_id):
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
        # GET repositoryId, resourcesId
        cursor.execute('SELECT "repositoryId", "resourcesId" FROM "Request" WHERE id = %s;', (request_id,))
        res = cursor.fetchone()
        if not res:
            raise ValueError(f"No request found for id={request_id}")
        repositoryId, resourcesId = res

        # Delete request record
        cursor.execute('DELETE FROM "Request" WHERE id = %s;', (request_id,))
        connection.commit()
        print(f"Deleted request with id={request_id}")

        # Delete repository record
        cursor.execute('DELETE FROM "Repository" WHERE id = %s;', (repositoryId,))
        connection.commit()
        print(f"Deleted repository with id={repositoryId}")

        # GET resourceConfigId
        cursor.execute('SELECT "resourceConfigId" FROM "Resources" WHERE id = %s;', (resourcesId,))
        res = cursor.fetchone()
        if not res:
            raise ValueError(f"No resources found for id={resourcesId}")
        resourceConfigId = res[0]

        # Delete Azure VM Instance
        cursor.execute('SELECT * FROM "AzureVMInstance" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
        res = cursor.fetchall()
        if res:
            cursor.execute('DELETE FROM "AzureVMInstance" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
            connection.commit()
            print(f"Deleted AzureVMInstance with resourceConfigId={resourceConfigId}")

        # Delete Azure Database Instance
        cursor.execute('SELECT * FROM "AzureDatabaseInstance" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
        res = cursor.fetchall()
        if res:
            cursor.execute('DELETE FROM "AzureDatabaseInstance" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
            connection.commit()
            print(f"Deleted AzureDatabaseInstance with resourceConfigId={resourceConfigId}")
    
        # Delete Azure Storage Instance
        cursor.execute('SELECT * FROM "AzureStorageInstance" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
        res = cursor.fetchall()
        if res:
            cursor.execute('DELETE FROM "AzureStorageInstance" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
            connection.commit()
            print(f"Deleted AzureStorageInstance with resourceConfigId={resourceConfigId}")

        # Delete Azure K8s Cluster
        cursor.execute('SELECT * FROM "AzureK8sCluster" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
        res = cursor.fetchall()
        if res:
            cursor.execute('DELETE FROM "AzureK8sCluster" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
            connection.commit()
            print(f"Deleted AzureK8sCluster with resourceConfigId={resourceConfigId}")

        # Delete Azure Resource Group
        # cursor.execute('SELECT * FROM "AzureResourceGroup" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
        # res = cursor.fetchall()
        # if res:
        #     cursor.execute('DELETE FROM "AzureResourceGroup" WHERE "resourceConfigId" = %s;', (resourceConfigId,))
        #     connection.commit()
        #     print(f"Deleted AzureResourceGroup with resourceConfigId={resourceConfigId}")

        # Delete Resources
        cursor.execute('DELETE FROM "Resources" WHERE id = %s;', (resourcesId,))
        connection.commit()
        print(f"Deleted Resources with id={resourcesId}")
        
        # Delete Resource Config
        cursor.execute('DELETE FROM "ResourceConfig" WHERE id = %s;', (resourceConfigId,))
        connection.commit()
        print(f"Deleted ResourceConfig with id={resourceConfigId}")

    finally:
        cursor.close()
        connection.close()

# -------------------------
# DAG Definition
# -------------------------
with DAG(
    dag_id='AZURE_Destroy',
    default_args=default_args,
    schedule=None,
    catchup=False,
    max_active_runs=1,
) as dag:
    
    # Get request ID
    get_request_id = PythonOperator(
        task_id="get_request_id",
        python_callable=rabbitmq_consumer,
    )

    # Get repository/project name
    get_repository_name = PythonOperator(
        task_id="get_repository_name",
        python_callable=repository_name,
        op_args=["{{ ti.xcom_pull(task_ids='get_request_id') }}"],
    )

    # Branch based on resource types
    branch_task = BranchPythonOperator(
        task_id='branch_resources',
        python_callable=branch_resources,
        op_args=["{{ ti.xcom_pull(task_ids='get_request_id') }}"],
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    # Destroy Azure VMs (same as existing)
    destroy_vm = BashOperator(
        task_id="terraform_destroy_vm",
        bash_command=(
            'cd "/opt/airflow/dags/terraform/{{ ti.xcom_pull(task_ids=\'get_repository_name\') | trim | replace(\'"\',\'\') }}/vm" && '
            'terraform init && terraform destroy -auto-approve'
        ),
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    # Destroy Azure Databases (same as existing)
    destroy_db = BashOperator(
        task_id="terraform_destroy_db",
        bash_command=(
            'cd "/opt/airflow/dags/terraform/{{ ti.xcom_pull(task_ids=\'get_repository_name\') | trim | replace(\'"\',\'\') }}/db" && '
            'terraform init && terraform destroy -auto-approve'
        ),
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    # Destroy Azure Storage (same as existing)
    destroy_st = BashOperator(
        task_id="terraform_destroy_st",
        bash_command=(
            'cd "/opt/airflow/dags/terraform/{{ ti.xcom_pull(task_ids=\'get_repository_name\') | trim | replace(\'"\',\'\') }}/st" && '
            'terraform init && terraform destroy -auto-approve'
        ),
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    # Destroy AKS Clusters (mimicking VM/DB/Storage pattern)
    destroy_k8s = BashOperator(
        task_id="terraform_destroy_k8s",
        bash_command=(
            'cd "/opt/airflow/dags/terraform/{{ ti.xcom_pull(task_ids=\'get_repository_name\') | trim | replace(\'"\',\'\') }}/k8s" && '
            'terraform init && terraform destroy -auto-approve'
        ),
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    # Destroy Azure Resource Group (must be last)
    # destroy_rg = BashOperator(
    #     task_id="terraform_destroy_rg",
    #     bash_command=(
    #         'cd "/opt/airflow/dags/terraform/{{ ti.xcom_pull(task_ids=\'get_repository_name\') | trim | replace(\'"\',\'\') }}/resource_group" && '
    #         'terraform init && terraform destroy -auto-approve'
    #     ),
    #     retries=3,
    #     retry_delay=timedelta(minutes=5),
    #     trigger_rule='none_failed_min_one_success'
    # )

    # Cleanup folder
    cleanup_dir = PythonOperator(
        task_id="cleanup_dir",
        python_callable=cleanup_directory,
        op_args=["{{ ti.xcom_pull(task_ids='get_repository_name') }}"],
        trigger_rule='none_failed_min_one_success',
    )

    # Delete DB record
    delete_request = PythonOperator(
        task_id='supabase_delete_request',
        python_callable=supabase_delete_request,
        op_args=["{{ ti.xcom_pull(task_ids='get_request_id') }}"],
        trigger_rule='all_done',
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    # Task dependencies
    get_request_id >> get_repository_name >> branch_task
    branch_task >> [destroy_vm, destroy_db, destroy_st, destroy_k8s] >> cleanup_dir >> delete_request





