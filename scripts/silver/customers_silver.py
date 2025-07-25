import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("Silver_Customers") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

bronze_path = "s3a://mf-atividadebucket/bronze/customers.parquet"
silver_path = "s3a://mf-atividadebucket/silver/customers.parquet"

df = spark.read.parquet(bronze_path)

df_silver = df.withColumnRenamed("customer_id", "id") \
              .withColumnRenamed("customer_fname", "first_name") \
              .withColumnRenamed("customer_lname", "last_name") \
              .withColumnRenamed("customer_email", "email") \
              .withColumnRenamed("customer_city", "city") \
              .withColumnRenamed("customer_state", "state")

df_silver.write.mode("overwrite").parquet(silver_path)

spark.stop()
