from datetime import datetime, timedelta

from airflow import DAG

from rudder_airflow_provider.operators.rudderstack import RudderstackRETLOperator

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
    # retl_connection_id, sync_type are template fields
    rs_operator = RudderstackRETLOperator(
        retl_connection_id="{{ var.value.retl_connection_id }}",
        task_id='<replace task id>',
        connection_id='<rudderstack api connection id>',
        sync_type="{{ var.value.sync_type }}",
        wait_for_completion=True
    )

if __name__ == "__main__":
    dag.test()