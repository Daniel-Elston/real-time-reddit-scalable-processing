from __future__ import annotations

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from config.pipeline_context import PipelineContext
from src.pipelines.extract_pipeline import ExtractionPipeline
from utils.execution import TaskExecutor

"""
SAMPLE DAG FILE:
Dags ran from /Airflow Directory
"""


default_args = {
    'owner': 'Daniel Elston',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_data_stream',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5)
)


def stream_data():
    ExtractionPipeline(
        ctx=PipelineContext(),
        exe=TaskExecutor()
    ).extract_real_time_data()


produce_task = PythonOperator(
    task_id='stream_to_kafka',
    python_callable=stream_data,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
