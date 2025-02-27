from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from typing import Dict, Any

default_args: Dict[str, Any] = {
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'airflow_monitoring',
    default_args=default_args,
    description='liveness monitoring dag',
    schedule_interval='*/10 * * * *',
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
)

t1: BashOperator = BashOperator(
    task_id='echo',
    bash_command='echo "Airflow is running"',
    dag=dag,
    depends_on_past=False,
    priority_weight=2**31 - 1,
    do_xcom_push=False
)
