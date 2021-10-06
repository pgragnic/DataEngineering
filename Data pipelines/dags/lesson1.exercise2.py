from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime

def hello_world():
    print('Hello World')

divvy_dag = DAG(
    'divvy',
    description='Analyses Divvy Bikeshare data',
    start_date=datetime.datetime.now(),
    schedule_interval='@daily'
    )

task = PythonOperator(
    task_id='hello_world',
    python_callable=hello_world,
    dag=divvy_dag)