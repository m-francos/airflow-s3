import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
    .appName("Silver_OrderItems") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

bronze_path = "s3a://mf-atividadebucket/bronze/order_items.parquet"
silver_path = "s3a://mf-atividadebucket/silver/order_items.parquet"

df = spark.read.parquet(bronze_path)

df_silver = df.withColumnRenamed("order_item_id", "id") \
              .withColumnRenamed("order_item_order_id", "order_id") \
              .withColumnRenamed("order_item_product_id", "product_id") \
              .withColumnRenamed("order_item_quantity", "quantity") \
              .withColumnRenamed("order_item_product_price", "price") \
              .withColumnRenamed("order_item_subtotal", "order_item_subtotal")

df_silver.write.mode("overwrite").parquet(silver_path)

spark.stop()
