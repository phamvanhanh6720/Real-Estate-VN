import os
from datetime import timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
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
    description="Run AWS Glue Crawlers to catalog data from Batdongsan data source",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=15),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["data lake", "batdongsan", "raw"],
) as dag:
    begin = EmptyOperator(task_id="begin")

    end = EmptyOperator(task_id="end")
    list_glue_tables = BashOperator(
        task_id="list_glue_tables",
        bash_command="""aws glue get-tables --database-name "real-estate-db" \
                          --query 'TableList[].Name' --expression "batdongsan_raw_*"  \
                          --output table""",
    )

    CRAWLERS = ['batdongsan_data.news_raw', 'batdongsan_data.projects_by_districts',
                'batdongsan_data.projects_raw', 'batdongsan_date_raw',
                'batdongsan_loc.cities_raw', 'batdongsan_loc.districts_raw']

    for crawler in CRAWLERS:
        crawlers_run = GlueCrawlerOperator(
            task_id=f'run_{crawler}_crawler',
            config={"Name": crawler},
            aws_conn_id="aws_user_datalake"
        )

        chain(
            begin,
            crawlers_run,
            list_glue_tables,
            end
        )
