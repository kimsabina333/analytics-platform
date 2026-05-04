import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def check_libs():
    result = subprocess.run(['pip', 'freeze'], capture_output=True, text=True)
    print(result.stdout)

with DAG('check_versions_dag', start_date=datetime(2023, 1, 1), schedule_interval='@once') as dag:
    task = PythonOperator(task_id='print_pip_freeze', python_callable=check_libs)