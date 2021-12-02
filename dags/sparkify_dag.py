from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries


default_args = {
    "owner": "sparkify",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 12),
    "retries": 1,
    "retries_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "sparkify_analysis",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@hourly",
    max_active_runs=1,
)

start_operator = DummyOperator(task_id="begin_execution", dag=dag)

# create all tables using create_tables.sql
create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql="create_tables.sql",
    postgres_conn_id="redshift",
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    aws_credential_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A",
)

# load_songplays_table = LoadFactOperator(task_id="load_songplays_fact_table", dag=dag)
songplays_table_columns = (
    "start_time, user_id, song_id, artist_id, level, session_id, location, user_agent"
)
load_songplays_table = LoadDimensionOperator(
    task_id="load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name="songplays",
    column_names=songplays_table_columns,
    selecting_query=SqlQueries.songplay_table_insert,
)

user_table_columns = "user_id, first_name, last_name, gender, level"
load_user_dimension_table = LoadDimensionOperator(
    task_id="load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name="user",
    column_names=user_table_columns,
    selecting_query=SqlQueries.user_table_insert,
)

song_table_columns = "song_id, title, artist_id, year, duration"
load_song_dimension_table = LoadDimensionOperator(
    task_id="load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name="song",
    column_names=song_table_columns,
    selecting_query=SqlQueries.song_table_insert,
)

artist_table_columns = "artist_id, name, location, latitude, longitude"
load_artist_dimension_table = LoadDimensionOperator(
    task_id="load_artist_dim_table",
    dag=dag,
    table_name="artist",
    redshift_conn_id="redshift",
    column_names=artist_table_columns,
    selecting_query=SqlQueries.artist_table_insert,
)

time_table_columns = "start_time, hour, day, week, month, year, weekday"
load_time_dimension_table = LoadDimensionOperator(
    task_id="load_time_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    column_names=time_table_columns,
    selecting_query=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(task_id="run_data_quality_checks", dag=dag)

end_operator = DummyOperator(task_id="stop_execution", dag=dag)

# Ordering the task in the pipeline
start_operator >> create_tables_task

create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift

stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table

load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_time_dimension_table
