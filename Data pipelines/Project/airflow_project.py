import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from operators.data_quality import DataQualityOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator

from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime.datetime.now(),
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=1),
    'catchup' : False,
    'email_on_retry': False
}

dag = DAG('airflow_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime.datetime.now()
          #schedule_interval='0 * * * *'
        )

# Dummy operators added for readbility of the DAG UI

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
    )

tables_created = DummyOperator(
    task_id='Tables_created',
    dag=dag
    )

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag)

# Create Tables (if not exists)

j = 0
for query in SqlQueries.create_table_queries:
    print(f"Processing query: {query}")
    j += 1
    dynamicTask = f"Create_table{j}"
    create_table = PostgresOperator(
        task_id=dynamicTask,
        dag=dag,
        postgres_conn_id="redshift",
        sql=query
    )
    start_operator >> create_table
    create_table >> tables_created

# Stage events table

stage_events_to_redshift = S3ToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket='udacity-dend',
    s3_key='log_data',
    table='staging_events',
    schema="PUBLIC",
    copy_options=["JSON 'auto ignorecase' ACCEPTINVCHARS"]
)

# Stage songs table

stage_songs_to_redshift = S3ToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_conn_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_bucket='udacity-dend',
    s3_key='song_data',
    table='staging_songs',
    schema="PUBLIC",
    copy_options=["JSON 'auto ignorecase' ACCEPTINVCHARS"]
)

# Load final tables

load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql_stmt=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    append=False,
    sql_stmt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    append=False,
    sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    append=False,
    sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    append=False,
    sql_stmt=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["songplays","users","songs","artists","time"] 
)

tables_created >> stage_events_to_redshift
tables_created >> stage_songs_to_redshift

stage_songs_to_redshift >> load_songplays_fact_table
stage_events_to_redshift  >> load_songplays_fact_table

load_songplays_fact_table >> load_user_dimension_table
load_songplays_fact_table >> load_song_dimension_table
load_songplays_fact_table >> load_artist_dimension_table
load_songplays_fact_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator