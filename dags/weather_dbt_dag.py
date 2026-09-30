from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="weather_dbt_dag",
    start_date=datetime(2026, 9, 1),
    schedule=None,  # triggered by weather_etl_dag, not on its own clock
    catchup=False,
    tags=["weather", "dbt"],
) as dag:

    dbt_deps = BashOperator(task_id="dbt_deps", bash_command=f"cd {DBT_DIR} && dbt deps")
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"cd {DBT_DIR} && dbt run")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"cd {DBT_DIR} && dbt test")
    dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=f"cd {DBT_DIR} && dbt snapshot")

    dbt_deps >> dbt_run >> dbt_test >> dbt_snapshot

