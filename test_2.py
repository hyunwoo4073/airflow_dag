from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# DAG 정의
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print(f"Hello from Airflow! Now is {datetime.now()}")

with DAG(
    dag_id='example_hello_dag',
    default_args=default_args,
    description='A simple DAG that prints hello',
    schedule_interval='@daily',  # 매일 실행
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    hello_task

