from datetime import datetime, timedelta

from airflow import DAG

from rudder_airflow_provider.operators.rudderstack import RudderstackOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('rudderstack-sample',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['rs']) as dag:
    rs_operator = RudderstackOperator(source_id='<source-id>', task_id='<any-task-id>')
    
    rs_operator
