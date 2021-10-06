# Instructions
# Define a function that uses the python logger to log a function. Then finish filling in the details of the DAG down below. Once you’ve done that, run "/opt/airflow/start.sh" command to start the web server. Once the Airflow web server is ready,  open the Airflow UI using the "Access Airflow" button. Turn your DAG “On”, and then Run your DAG. If you get stuck, you can take a look at the solution file in the workspace/airflow/dags folder in the workspace and the video walkthrough on the next page.

import datetime
import logging
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def hello_world():
        logging.info("Hello World!")

def current_time():
        logging.info("Current time is {dateime.datetime.now().isoformat()}")

def working_dir():
        logging.info("Working directory is {os.getcwd()}")

def complete():
        logging.info("Congrats, your first multi-task pipeline is now complete!")

dag = DAG(
        'lesson1.demo3',
        start_date=datetime.datetime.now() - datetime.timedelta(days=1),
        schedule_interval = '@hourly'
        )

#
# TODO: Uncomment the operator below and replace the arguments labeled <REPLACE> below
#

hello_world_task = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world,
    dag=dag
)

current_time_task = PythonOperator(
    task_id="current_time",
    python_callable=current_time,
    dag=dag
)

working_dir_task = PythonOperator(
    task_id="working_dir",
    python_callable=working_dir,
    dag=dag
)

complete_task = PythonOperator(
    task_id="complete",
    python_callable=complete,
    dag=dag
)

#                    ->  current_time_task
#                   /                     \
#   hello_world_task                       -> division_task
#                   \                     /
#                    ->  working_dir_task

hello_world_task >> current_time_task
hello_world_task >> working_dir_task
current_time_task >> complete_task
working_dir_task >> complete_task