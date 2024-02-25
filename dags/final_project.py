from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from operators.data_quality import (TableIsEmptyCheck, QueryHaveExpectedResultCheck)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)
def final_project():
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        aws_credentials_id="aws_credentials",
        redshift_connection_id="redshift",
        table="staging_events",
        s3_bucket="udacity-dag-bucket-test",
        s3_path= "log-data",
        json_path="s3://udacity-dag-bucket-test/log_json_path.json",
    )
    
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        aws_credentials_id="aws_credentials",
        redshift_connection_id="redshift",
        table="staging_songs",
        s3_bucket="udacity-dag-bucket-test",
        s3_path= "song-data",
        json_path="auto",
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        redshift_connection_id="redshift",
        insert_sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        redshift_connection_id="redshift",
        insert_sql_query=SqlQueries.user_table_insert,
        truncate_before_insert=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        redshift_connection_id="redshift",
        insert_sql_query=SqlQueries.song_table_insert,
        truncate_before_insert=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        redshift_connection_id="redshift",
        insert_sql_query=SqlQueries.artist_table_insert,
        truncate_before_insert=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        redshift_connection_id="redshift",
        insert_sql_query=SqlQueries.time_table_insert,
        truncate_before_insert=True
    )

    dq_checks=[
        TableIsEmptyCheck("songplays"),
        TableIsEmptyCheck("users"),
        TableIsEmptyCheck("songs"),
        TableIsEmptyCheck("artists"),
        TableIsEmptyCheck("time"),
        QueryHaveExpectedResultCheck("SELECT COUNT(*) FROM songplays WHERE songplay_id is null", 0),
        QueryHaveExpectedResultCheck("SELECT COUNT(*) FROM users WHERE user_id is null", 0),
        QueryHaveExpectedResultCheck("SELECT COUNT(*) FROM songs WHERE song_id is null", 0),
        QueryHaveExpectedResultCheck("SELECT COUNT(*) FROM artists WHERE artist_id is null", 0),
        QueryHaveExpectedResultCheck("SELECT COUNT(*) FROM time WHERE start_time is null", 0)
    ]
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_connection_id="redshift",
        checks=dq_checks
    ) 

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

    [stage_songs_to_redshift, stage_events_to_redshift] >> load_songplays_table

    load_songplays_table >> [load_user_dimension_table,load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]

    [load_user_dimension_table, load_song_dimension_table, 
     load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()