import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime, timedelta
from dotenv import load_dotenv
from os.path import expanduser

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

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

ANSIBLE_BASE = "/opt/airflow/dags/ansible"
INVENTORY_PATH = f"{ANSIBLE_BASE}/inventory/hosts.ini"

# --------------------------------------------------
# Step 1: Fetch cluster info from DB
# --------------------------------------------------
def fetch_cluster_info(**context):
    resource_id = context["dag_run"].conf.get("resource_id")
    if not resource_id:
        raise ValueError("resource_id missing")

    load_dotenv(expanduser("/opt/airflow/dags/.env"))

    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
    )
    cur = conn.cursor()

    cur.execute(
        'SELECT "resourceConfigId" FROM "Resources" WHERE id = %s;',
        (resource_id,),
    )
    res = cur.fetchone()
    if not res:
        raise ValueError("Resource not found")

    resource_config_id = res[0]

    cur.execute(
        '''
        SELECT "id", "clusterName", "clusterEndpoint", "terraformState"
        FROM "AwsK8sCluster"
        WHERE "resourceConfigId" = %s;
        ''',
        (resource_config_id,),
    )
    clusters = cur.fetchall()

    cur.close()
    conn.close()

    if not clusters:
        raise ValueError("No clusters found")

    cluster_data = []

    for cid, name, endpoint, tf_state in clusters:
        endpoint = json.loads(endpoint) if endpoint else {}
        tf_state = json.loads(tf_state) if tf_state else {}

        workers = []
        edge = None

        for r in tf_state.get("resources", []):
            if r.get("type") == "aws_instance":
                if r.get("name") == "k3s_worker":
                    for i in r.get("instances", []):
                        workers.append(i["attributes"])
                if r.get("name") == "k3s_edge":
                    edge = r["instances"][0]["attributes"]

        cluster_data.append({
            "cluster_id": cid,
            "cluster_name": name,
            "master": endpoint,
            "workers": workers,
            "edge": edge,
        })

    return cluster_data


# --------------------------------------------------
# Step 2: Generate Ansible inventory
# --------------------------------------------------
def generate_inventory(**context):
    clusters = context["ti"].xcom_pull(task_ids="fetch_cluster_info")

    Path(os.path.dirname(INVENTORY_PATH)).mkdir(parents=True, exist_ok=True)

    lines = ["[k3s_master]"]
    master = clusters[0]["master"]

    lines.append(
        f"master ansible_host={master['public_ip']} ansible_user=ubuntu"
    )

    lines.append("\n[k3s_workers]")
    for idx, w in enumerate(clusters[0]["workers"]):
        lines.append(
            f"worker{idx+1} ansible_host={w['public_ip']} ansible_user=ubuntu"
        )

    lines.append("\n[edge]")
    edge = clusters[0]["edge"]
    if edge:
        lines.append(
            f"edge ansible_host={edge['public_ip']} ansible_user=ubuntu"
        )

    with open(INVENTORY_PATH, "w") as f:
        f.write("\n".join(lines))

    return INVENTORY_PATH


# --------------------------------------------------
# Step 3: Fetch kubeconfig & store in DB
# --------------------------------------------------
def fetch_kubeconfig(**context):
    clusters = context["ti"].xcom_pull(task_ids="fetch_cluster_info")
    master_ip = clusters[0]["master"]["public_ip"]

    load_dotenv(expanduser("/opt/airflow/dags/.env"))

    kubeconfig_path = "/opt/airflow/dags/tmp_kubeconfig"

    os.system(
        f"ssh -o StrictHostKeyChecking=no "
        f"-i /opt/airflow/dags/.ssh/id_rsa "
        f"ubuntu@{master_ip} "
        f"'cat /home/ubuntu/.kube/config' > {kubeconfig_path}"
    )

    with open(kubeconfig_path) as f:
        kubeconfig = f.read()

    conn = psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
    )
    cur = conn.cursor()

    cur.execute(
        '''
        UPDATE "AwsK8sCluster"
        SET "kubeConfig" = %s
        WHERE id = %s;
        ''',
        (kubeconfig, clusters[0]["cluster_id"]),
    )

    conn.commit()
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
    description="Configure K3s cluster using Ansible",
) as dag:

    fetch_info = PythonOperator(
        task_id="fetch_cluster_info",
        python_callable=fetch_cluster_info,
    )

    write_inventory = PythonOperator(
        task_id="generate_ansible_inventory",
        python_callable=generate_inventory,
    )

    ansible_master = BashOperator(
        task_id="ansible_master",
        bash_command=f"""
        cd {ANSIBLE_BASE} &&
        ansible-playbook playbooks/master.yml
        """,
    )

    ansible_worker = BashOperator(
        task_id="ansible_worker",
        bash_command=f"""
        cd {ANSIBLE_BASE} &&
        ansible-playbook playbooks/worker.yml
        """,
    )

    ansible_edge = BashOperator(
        task_id="ansible_edge",
        bash_command=f"""
        cd {ANSIBLE_BASE} &&
        ansible-playbook playbooks/edge.yml
        """,
    )

    store_kubeconfig = PythonOperator(
        task_id="fetch_kubeconfig",
        python_callable=fetch_kubeconfig,
    )

    fetch_info >> write_inventory >> ansible_master >> ansible_worker >> ansible_edge >> store_kubeconfig
