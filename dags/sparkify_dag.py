from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries


default_args = {
    "owner": "sparkify",
    "start_date": datetime(2019, 1, 12),
}

dag = DAG(
    "sparkify_analysis",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)

start_operator = DummyOperator(task_id="begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(task_id="stage_events", dag=dag)

stage_songs_to_redshift = StageToRedshiftOperator(task_id="stage_songs", dag=dag)

load_songplays_table = LoadFactOperator(task_id="load_songplays_fact_table", dag=dag)

load_user_dimension_table = LoadDimensionOperator(
    task_id="load_user_dim_table", dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="load_song_dim_table", dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="load_artist_dim_table", dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="load_time_dim_table", dag=dag
)

run_quality_checks = DataQualityOperator(task_id="run_data_quality_checks", dag=dag)

end_operator = DummyOperator(task_id="stop_execution", dag=dag)
