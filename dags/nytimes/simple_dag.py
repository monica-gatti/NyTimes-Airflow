from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime
import time

simple_dag = DAG(
    dag_id='too_long_dag',
    description='My DAG that\'s triggered every minute',
    tags=['tutorial', 'datascientest'],
    schedule_interval=datetime.timedelta(seconds=10),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
)


def sleep_20_seconds():
    time.sleep(20)


my_task = PythonOperator(
    task_id='sleep_20_seconds',
    dag=simple_dag,
    python_callable=sleep_20_seconds
)
