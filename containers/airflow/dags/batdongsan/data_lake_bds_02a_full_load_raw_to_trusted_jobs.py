import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "phamvanhanh6720",
    "depends_on_past": False,
    "retries": 0,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Run AWS Glue ETL Jobs - Batdongsan - Raw to Trusted Data",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["data lake", "batdongsan", "raw"],
) as dag:
    begin = EmptyOperator(task_id="begin")

    end = EmptyOperator(task_id="end")

    task_1 = GlueJobOperator(
        task_id="batdongsan_raw_to_trusted_loc.districts",
        job_name="batdongsan_raw_to_trusted_loc.districts",
        aws_conn_id="aws_user_datalake"
    )

    task_2 = GlueJobOperator(
        task_id="batdongsan_raw_to_trusted_date",
        job_name="batdongsan_raw_to_trusted_date",
        aws_conn_id="aws_user_datalake"
    )

    task_3 = GlueJobOperator(
        task_id="batdongsan_raw_to_trusted_data.projects",
        job_name="batdongsan_raw_to_trusted_data.projects",
        aws_conn_id="aws_user_datalake"
    )

    task_5 = GlueJobOperator(
        task_id="batdongsan_raw_to_trusted_data.cities",
        job_name="batdongsan_raw_to_trusted_data.cities",
        aws_conn_id="aws_user_datalake"
    )

    task_7 = GlueJobOperator(
        task_id="batdongsan_address_trusted",
        job_name="batdongsan_address_trusted",
        aws_conn_id="aws_user_datalake"
    )

    begin >> [task_1, task_2, task_5] >> task_3 >> task_7 >> end

