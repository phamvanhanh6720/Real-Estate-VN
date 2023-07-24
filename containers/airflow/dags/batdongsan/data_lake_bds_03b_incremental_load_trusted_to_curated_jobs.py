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

    dim_user_task = GlueJobOperator(
        task_id="batdongsan_dim_user",
        job_name="batdongsan_dim_user",
        aws_conn_id="aws_user_datalake"
    )

    fact_task = GlueJobOperator(
        task_id="batdongsan_fact_real_estate_news",
        job_name="batdongsan_fact_real_estate_news",
        aws_conn_id="aws_user_datalake"
    )

    delete_wide_table = S3DeleteObjectsOperator(
        task_id="delete_dw_wide_table",
        bucket=Variable.get('s3_bucket_data_lake'),
        prefix='curated/wide_table/fact_news_wide_table/',
        aws_conn_id="aws_user_datalake"
    )

    wide_table_task = GlueJobOperator(
        task_id="batdongsan_dw_wide_table",
        job_name="batdongsan_dw_wide_table",
        aws_conn_id="aws_user_datalake"
    )

    begin >> dim_user_task >> fact_task >> delete_wide_table >> wide_table_task >> end

