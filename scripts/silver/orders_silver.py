import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("Silver_Orders") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

bronze_path = "s3a://mf-atividadebucket/bronze/orders.parquet"
silver_path = "s3a://mf-atividadebucket/silver/orders.parquet"

df = spark.read.parquet(bronze_path)

df_silver = df.withColumnRenamed("order_id", "id") \
              .withColumnRenamed("order_date", "date") \
              .withColumnRenamed("order_customer_id", "customer_id") \
              .withColumnRenamed("order_status", "status")

df_silver.write.mode("overwrite").parquet(silver_path)

spark.stop()
