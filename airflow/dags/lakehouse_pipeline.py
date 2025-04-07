from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import os

DAG_PATH = os.path.dirname(os.path.abspath(__file__))
BASE_PATH = os.path.dirname(os.path.dirname(DAG_PATH))
SCRIPTS_PATH = os.path.join(BASE_PATH, "scripts")

default_args = {
    'owner': 'maite',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline Lakehouse com caminhos dinâmicos',
    schedule_interval=None,
    catchup=False,
) as dag:

    bronze_customers = BashOperator(
        task_id='bronze_customers',
        bash_command=f'spark-submit {os.path.join(SCRIPTS_PATH, "bronze", "bronze_customers.py")}'
    )

    bronze_orders = BashOperator(
        task_id='bronze_orders',
        bash_command=f'spark-submit {os.path.join(SCRIPTS_PATH, "bronze", "bronze_orders.py")}'
    )

    bronze_order_items = BashOperator(
        task_id='bronze_order_items',
        bash_command=f'spark-submit {os.path.join(SCRIPTS_PATH, "bronze", "bronze_order_items.py")}'
    )

    silver_customers = BashOperator(
        task_id='silver_customers',
        bash_command=f'spark-submit {os.path.join(SCRIPTS_PATH, "silver", "customers_silver.py")}'
    )

    silver_orders = BashOperator(
        task_id='silver_orders',
        bash_command=f'spark-submit {os.path.join(SCRIPTS_PATH, "silver", "orders_silver.py")}'
    )

    silver_order_items = BashOperator(
        task_id='silver_order_items',
        bash_command=f'spark-submit {os.path.join(SCRIPTS_PATH, "silver", "order_items_silver.py")}'
    )

    gold = BashOperator(
        task_id='gold',
        bash_command=f'spark-submit {os.path.join(SCRIPTS_PATH, "gold_processing.py")}'
    )

    [bronze_customers, bronze_orders, bronze_order_items] >> silver_customers
    [bronze_customers, bronze_orders, bronze_order_items] >> silver_orders
    [bronze_customers, bronze_orders, bronze_order_items] >> silver_order_items

    [silver_customers, silver_orders, silver_order_items] >> gold

