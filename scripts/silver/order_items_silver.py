import os
from pyspark.sql import SparkSession

base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
bronze_path = os.path.join(base_path, "data", "bronze", "order_items.parquet")
silver_path = os.path.join(base_path, "data", "silver", "order_items.parquet")

spark = SparkSession.builder.appName("Silver_OrderItems").getOrCreate()

df = spark.read.parquet(bronze_path)

df_silver = df.withColumnRenamed("order_item_id", "id") \
              .withColumnRenamed("order_item_order_id", "order_id") \
              .withColumnRenamed("order_item_product_id", "product_id") \
              .withColumnRenamed("order_item_quantity", "quantity") \
              .withColumnRenamed("order_item_product_price", "price")

df_silver.write.mode("overwrite").parquet(silver_path)

spark.stop()

