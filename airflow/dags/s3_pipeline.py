from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

SPARK_HOME = os.getenv("SPARK_HOME")
if not SPARK_HOME:
    raise EnvironmentError("A variável de ambiente SPARK_HOME não está definida.")

DAG_PATH = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(DAG_PATH))
SCRIPTS_PATH = os.path.join(PROJECT_ROOT, "scripts")

spark_submit = os.path.join(SPARK_HOME, "bin/spark-submit")
python_path = sys.executable

aws_conn = BaseHook.get_connection("aws_default")
aws_access_key = aws_conn.login or ""
aws_secret_key = aws_conn.password or ""

default_args = {
    "owner": "maite",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

spark_base_cmd = (
    f"{spark_submit} "
    "--master local[*] "
    "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.508 "
    "--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
    "--conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com "
    f'--conf spark.pyspark.python="{python_path}" '
    f'--conf spark.pyspark.driver.python="{python_path}" '
)

def spark_task(task_id, rel_path):
    script_path = os.path.join(SCRIPTS_PATH, rel_path)
    return BashOperator(
        task_id=task_id,
        bash_command=f"{spark_base_cmd}{script_path}",
        env={
            "SPARK_HOME": SPARK_HOME,
            "AWS_ACCESS_KEY_ID": aws_access_key,
            "AWS_SECRET_ACCESS_KEY": aws_secret_key,
        },
    )

with DAG(
    dag_id="s3_pipeline",
    default_args=default_args,
    description="Pipeline Lakehouse com S3",
    schedule=None,
    catchup=False,
) as dag:
    bronze_customers = spark_task("bronze_customers", "bronze/bronze_customers.py")
    bronze_orders = spark_task("bronze_orders", "bronze/bronze_orders.py")
    bronze_order_items = spark_task("bronze_order_items", "bronze/bronze_order_items.py")

    silver_customers = spark_task("silver_customers", "silver/customers_silver.py")
    silver_orders = spark_task("silver_orders", "silver/orders_silver.py")
    silver_order_items = spark_task("silver_order_items", "silver/order_items_silver.py")

    gold = spark_task("gold", "gold_processing.py")

    [bronze_customers, bronze_orders, bronze_order_items] >> silver_customers
    [bronze_customers, bronze_orders, bronze_order_items] >> silver_orders
    [bronze_customers, bronze_orders, bronze_order_items] >> silver_order_items
    [silver_customers, silver_orders, silver_order_items] >> gold