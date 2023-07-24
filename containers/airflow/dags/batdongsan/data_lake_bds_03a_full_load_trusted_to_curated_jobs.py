import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
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
    description="Run AWS Glue ETL Jobs - Batdongsan - Full Load Trusted to Curated Data",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["data lake", "batdongsan", "trusted"],
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    delete_folder_dim_date = S3DeleteObjectsOperator(
        task_id="delete_dim_date",
        bucket=Variable.get('s3_bucket_data_lake'),
        prefix='curated/dim/dim_date/',
        aws_conn_id="aws_user_datalake"
    )

    delete_folder_dim_project = S3DeleteObjectsOperator(
        task_id="delete_dim_project",
        bucket=Variable.get('s3_bucket_data_lake'),
        prefix='curated/dim/dim_project/',
        aws_conn_id="aws_user_datalake"
    )

    delete_folder_dim_address = S3DeleteObjectsOperator(
        task_id="delete_dim_address",
        bucket=Variable.get('s3_bucket_data_lake'),
        prefix='curated/dim/dim_address/',
        aws_conn_id="aws_user_datalake"
    )

    dim_address_task = GlueJobOperator(
        task_id="batdongsan_dim_address",
        job_name="batdongsan_dim_address",
        aws_conn_id="aws_user_datalake"
    )

    dim_date_task = GlueJobOperator(
        task_id="batdongsan_dim_date",
        job_name="batdongsan_dim_date",
        aws_conn_id="aws_user_datalake"
    )

    dim_project_task = GlueJobOperator(
        task_id="batdongsan_dim_project",
        job_name="batdongsan_dim_project",
        aws_conn_id="aws_user_datalake"
    )

    begin.set_downstream([delete_folder_dim_date, delete_folder_dim_address, delete_folder_dim_project])

    delete_folder_dim_date.set_downstream(dim_date_task)
    delete_folder_dim_address.set_downstream(dim_address_task)
    delete_folder_dim_project.set_downstream(dim_project_task)

    dim_date_task.set_downstream(end)
    dim_address_task.set_downstream(end)
    dim_project_task.set_downstream(end)
