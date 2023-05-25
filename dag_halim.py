from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.operators.python_operator import PythonOperator

import sys

from halim.consume import merge_data
from halim.datamart_daily_gross_revenue import daily_gross_revenue
from halim.datamart_monthly_gross_revenue_product_level import monthly_gross_revenue_product
from halim.datamart_monthly_orders_category_level import monthly_orders_category_level
from halim.datamart_monthly_orders_city_level import monthly_orders_city_level
from halim.datamart_monthly_orders_product_level import  monthly_orders_product_level


default_args = {
    'owner': 'halim',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
    'email': ['halim.iskandar2323@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

with DAG(
    dag_id="dag_halim",
    default_args=default_args,
    description="DAG to schedule merge_data function",
    schedule_interval="0 0 * * *",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:

    merge_data_task = PythonOperator(
        task_id="halim_merge_data_task",
        python_callable=merge_data,
    )

    daily_gross_revenue_task = PythonOperator(
        task_id="halim_create_daily_gross_revenue_task",
        python_callable=daily_gross_revenue,
    )

    monthly_gross_revenue_product_task = PythonOperator(
        task_id="halim_create_monthly_gross_revenue_product_task",
        python_callable=monthly_gross_revenue_product,
    )

    monthly_orders_category_level_task = PythonOperator(
        task_id="halim_create_monthly_orders_category_level_task",
        python_callable=monthly_orders_category_level,
    )

    monthly_orders_city_level_task = PythonOperator(
        task_id="halim_create_monthly_orders_city_level_task",
        python_callable=monthly_orders_city_level,
    )

    monthly_orders_product_level_task = PythonOperator(
        task_id="halim_create_monthly_orders_product_level_task",
        python_callable=monthly_orders_product_level,
    )

    merge_data_task >> [daily_gross_revenue_task,monthly_gross_revenue_product_task,monthly_orders_category_level_task,monthly_orders_city_level_task,monthly_orders_product_level_task]

if __name__ == "__main__":
    dag.test()