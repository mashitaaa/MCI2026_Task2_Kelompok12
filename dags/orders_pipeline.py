"""
orders_pipeline.py
DAG Airflow: Fetch orders API → Preprocessing → ClickHouse → Metabase
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from fetch_orders import fetch_orders
from load_to_clickhouse import load_to_clickhouse

default_args = {
    "owner": "mci2026",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="orders_pipeline",
    default_args=default_args,
    description="Pipeline: Fetch orders API → Preprocessing → ClickHouse → Metabase",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mci2026", "orders", "clickhouse"],
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_orders",
        python_callable=fetch_orders,
    )

    task_load = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=load_to_clickhouse,
    )

    task_fetch >> task_load
