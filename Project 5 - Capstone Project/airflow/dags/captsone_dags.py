from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
#from airflow.contrib.hooks.ssh_hook import SSHHook
#from airflow.contrib.operators.ssh_operator import SSHOperator

from operators import (
   StageToRedshiftOperator,
   LoadFactOperator,
   LoadDimensionOperator,
   DataQualityOperator
)

from helpers import SqlQueries
# Define default arguments


default_args = {
    'owner': 'udacity',
    'start_date' : datetime(2016, 2, 19, 0, 0, 0, 0),
    'end_date' : datetime(2017, 2, 19, 0, 0, 0, 0),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

dag = DAG('immigration_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly'
        )

# Define dag variables
processed_dataset = 'immigration_processed_files'
bucket = 'udacitycapstoneproject'

start_pipeline = DummyOperator(task_id='Begin_execution',  dag=dag)

#emrsshHook= SSHHook(ssh_conn_id='emr_ssh_connection')

#jobOperator = SSHOperator(
  #  task_id="ImmigrationETLJob",
 #   command='cd /home/hadoop/immigration_etl_pipeline/src;export PYSPARK_DRIVER_PYTHON=python3;export PYSPARK_PYTHON=python3;spark-submit --master yarn immigration_driver.py;',
   # ssh_hook=emrsshHook,
   # dag=dag)

stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id="stage_immigration_to_redshift",
    dag=dag,
    table="staging_immigration",
    redshift_conn_id="redshift",
    s3_bucket=bucket,
    s3_key='immigration_processed_files/sasdata/',
    IAM_ROLE='arn:aws:iam::972068528963:role/RedshiftCopyUnload',
    file_format='parquet',
    create_stmt=SqlQueries.create_table_staging_immigration
)

stage_us_cities_demographics_to_redshift = StageToRedshiftOperator(
    task_id="stage_us_cities_demographics_to_redshift",
    dag=dag,
    table="staging_us_cities_demographics",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket,
    s3_key='immigration_processed_files/demo/us-cities-demographics.csv',
    file_format='csv',
    create_stmt=SqlQueries.create_table_staging_us_cities_demographics
)

stage_airport_to_redshift = StageToRedshiftOperator(
    task_id="stage_airport_to_redshift",
    dag=dag,
    table="staging_airport",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket,
    s3_key='immigration_processed_files/airport/airport-codes_csv.csv',
    file_format='csv',
    create_stmt=SqlQueries.create_table_staging_airport
    
)

loaded_data_to_staging = DummyOperator(
   task_id = 'loaded_data_to_staging',
    dag=dag
)


load_country = StageToRedshiftOperator(
    task_id="load_country",
    dag=dag,
    table="d_country",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket,
    s3_key='immigration_processed_files/rescitycntry/I94City_Res.csv',
    file_format='csv',
    create_stmt=SqlQueries.create_table_d_country
)

load_state = StageToRedshiftOperator(
    task_id="load_state",
    dag=dag,
    table="d_state",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket,
    s3_key='immigration_processed_files/addrstate/I94ADDR_State.csv',
    file_format='csv',
    create_stmt=SqlQueries.create_table_d_state
    
)
load_port = StageToRedshiftOperator(
    task_id="load_port",
    dag=dag,
    table="d_port",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=bucket,
    s3_key='immigration_processed_files/port/I94_Port.csv',
    file_format='csv',
    create_stmt=SqlQueries.create_table_d_port
    
)

# Check loaded data not null


run_quality_checks_airports = DataQualityOperator(
    task_id="run_quality_checks_airports",
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM staging_airport WHERE ident is null", 'expected_result': 0}
       ]
)

run_quality_checks_us_cities_demo = DataQualityOperator(
    task_id="run_quality_checks_us_cities_demo",
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM staging_us_cities_demographics WHERE city is null", 'expected_result': 0}
    ]

)

run_quality_checks_immigration = DataQualityOperator(
    task_id="run_quality_checks_immigration",
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM staging_immigration WHERE cicid is null", 'expected_result': 0}
    ]

)


create_final_immigration_table = LoadFactOperator(
    task_id="create_final_immigration_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="final_immigration",
    create_stmt=SqlQueries.create_table_final_immigration,
    insert_stmt=SqlQueries.final_immigration_table_insert
)

run_quality_checks_final_immigration_data = DataQualityOperator(
    task_id="run_quality_checks_final_immigration_data",
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM final_immigration WHERE cicid is null", 'expected_result': 0}
    ]

)

create_D_CITY_DEMO = LoadDimensionOperator(
    task_id="create_D_CITY_DEMO",
    dag=dag,
    redshift_conn_id="redshift",
    append_data=True,
    table="D_CITY_DEMO",
    create_stmt=SqlQueries.create_table_D_CITY_DEMO,
    insert_stmt=SqlQueries.D_CITY_DEMO_INSERT

)


create_d_airport = LoadDimensionOperator(
    task_id="create_d_airport",
    dag=dag,
    redshift_conn_id="redshift",
    append_data=True,
    table="D_AIRPORT",
    create_stmt=SqlQueries.create_table_D_AIRPORT,
    insert_stmt=SqlQueries.D_AIRPORT_INSERT
)

create_d_time = LoadDimensionOperator(
    task_id="create_d_time",
    dag=dag,
    redshift_conn_id="redshift",
    append_data=True,
    table="D_TIME",
    create_stmt=SqlQueries.create_table_D_TIME,
    insert_stmt=SqlQueries.D_TIME_INSERT
)




finish_pipeline = DummyOperator(task_id='Stop_execution',  dag=dag)

dag >> start_pipeline  >> [stage_immigration_to_redshift,stage_us_cities_demographics_to_redshift,stage_airport_to_redshift]

stage_immigration_to_redshift >> run_quality_checks_immigration
stage_us_cities_demographics_to_redshift >> run_quality_checks_us_cities_demo
stage_airport_to_redshift >> run_quality_checks_airports


[run_quality_checks_immigration, run_quality_checks_us_cities_demo, run_quality_checks_airports]  >> loaded_data_to_staging

loaded_data_to_staging >> [load_country, load_port, load_state] >> create_final_immigration_table >> run_quality_checks_final_immigration_data

run_quality_checks_final_immigration_data >> [create_d_time, create_d_airport, create_D_CITY_DEMO]  >> finish_pipeline
