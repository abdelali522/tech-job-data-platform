from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "tech-job",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="tech_job_ingestion",
    description="Fetch CleanJobData jobs and publish them to Kafka for Spark processing.",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["tech-job", "ingestion", "kafka"],
) as dag:
    publish_jobs_to_kafka = BashOperator(
        task_id="publish_jobs_to_kafka",
        bash_command="cd /opt/project && python ingestion/api_ingestion.py",
        env={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
        },
        append_env=True,
    )
