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
songplays_table_columns = "playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent"
load_songplays_table = LoadFactOperator(
    task_id="load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name="songplays",
    column_names=songplays_table_columns,
    selecting_query=SqlQueries.songplay_table_insert,
)

user_table_columns = "userid, first_name, last_name, gender, level"
load_user_dimension_table = LoadDimensionOperator(
    task_id="load_user_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name="users",
    column_names=user_table_columns,
    selecting_query=SqlQueries.user_table_insert,
)

song_table_columns = "songid, title, artistid, year, duration"
load_song_dimension_table = LoadDimensionOperator(
    task_id="load_song_dim_table",
    dag=dag,
    redshift_conn_id="redshift",
    table_name="songs",
    column_names=song_table_columns,
    selecting_query=SqlQueries.song_table_insert,
)

artist_table_columns = "artistid, name, location, latitude, longitude"
load_artist_dimension_table = LoadDimensionOperator(
    task_id="load_artist_dim_table",
    dag=dag,
    table_name="artists",
    redshift_conn_id="redshift",
    column_names=artist_table_columns,
    selecting_query=SqlQueries.artist_table_insert,
)

time_table_columns = "start_time, hour, day, week, month, year, weekday"
load_time_dimension_table = LoadDimensionOperator(
    task_id="load_time_dim_table",
    table_name="time",
    dag=dag,
    redshift_conn_id="redshift",
    column_names=time_table_columns,
    selecting_query=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id="run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    fact_table="songplays",
    dim_tables=["artists", "users", "time", "songs"],
    staging_table="staging_songs",
    check_table="users",
    check_column="userid",
)

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

load_artist_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
