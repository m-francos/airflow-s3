from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'maite',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'lakehouse_data_pipeline',
    default_args=default_args,
    description='Pipeline completo para processamento Lakehouse',
    schedule_interval=None,
    catchup=False,
    tags=['lakehouse', 'databricks', 'pyspark'],
)

bronze_customers = BashOperator(
    task_id='bronze_customers',
    bash_command='spark-submit /home/maite/lakehouse/scripts/bronze/bronze_customers.py',
    dag=dag,
)

bronze_orders = BashOperator(
    task_id='bronze_orders',
    bash_command='spark-submit /home/maite/lakehouse/scripts/bronze/bronze_orders.py',
    dag=dag,
)

bronze_order_items = BashOperator(
    task_id='bronze_order_items',
    bash_command='spark-submit /home/maite/lakehouse/scripts/bronze/bronze_order_items.py',
    dag=dag,
)

silver_customers = BashOperator(
    task_id='silver_customers',
    bash_command='spark-submit /home/maite/lakehouse/scripts/silver/customers_silver.py',
    dag=dag,
)

silver_orders = BashOperator(
    task_id='silver_orders',
    bash_command='spark-submit /home/maite/lakehouse/scripts/silver/orders_silver.py',
    dag=dag,
)

silver_order_items = BashOperator(
    task_id='silver_order_items',
    bash_command='spark-submit /home/maite/lakehouse/scripts/silver/order_items_silver.py',
    dag=dag,
)

gold_processing = BashOperator(
    task_id='gold_processing',
    bash_command='spark-submit /home/maite/lakehouse/scripts/gold_processing.py',
    dag=dag,
)


[bronze_customers, bronze_orders, bronze_order_items] >> silver_customers
[bronze_customers, bronze_orders, bronze_order_items] >> silver_orders
[bronze_customers, bronze_orders, bronze_order_items] >> silver_order_items

[silver_customers, silver_orders, silver_order_items] >> gold_processing
