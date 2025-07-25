import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("BronzeOrderItems") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

s3_input_path = "s3a://mf-atividadebucket/landing/order_items.json"
s3_output_path = "s3a://mf-atividadebucket/bronze/order_items.parquet"

df = spark.read.json(s3_input_path)

df.show()

df.write.mode("overwrite").parquet(s3_output_path)

spark.stop()
