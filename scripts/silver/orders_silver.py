import os
from pyspark.sql import SparkSession

base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
bronze_path = os.path.join(base_path, "data", "bronze", "orders.parquet")
silver_path = os.path.join(base_path, "data", "silver", "orders.parquet")

spark = SparkSession.builder.appName("Silver_Orders").getOrCreate()

df = spark.read.parquet(bronze_path)

df_silver = df.withColumnRenamed("order_id", "id") \
              .withColumnRenamed("order_date", "date") \
              .withColumnRenamed("order_customer_id", "customer_id") \
              .withColumnRenamed("order_status", "status")

df_silver.write.mode("overwrite").parquet(silver_path)

spark.stop()

