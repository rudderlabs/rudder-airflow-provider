from datetime import datetime, timedelta

from airflow import DAG

from rudder_airflow_provider.operators.rudderstack import RudderstackETLOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "rudderstack-etl-sample",
    default_args=default_args,
    description="A simple tutorial DAG",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["rs-etl"],
) as dag:
    # etl_source_id is template field
    rs_operator = RudderstackETLOperator(
        etl_source_id="{{ var.value.etl_source_id }}",
        task_id="<a unique, meaningful id for the airflow task",
        connection_id="<rudderstack api connection id>",
    )

if __name__ == "__main__":
    dag.test()
