import os
from pyspark.sql import SparkSession

base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
landing_path = os.path.join(base_path, "data", "landing", "order_items.json")
bronze_path = os.path.join(base_path, "data", "bronze", "order_items.parquet")

spark = SparkSession.builder.appName("BronzeOrderItems").getOrCreate()

df = spark.read.json(landing_path)

df.show()

df.write.mode("overwrite").parquet(bronze_path)

spark.stop()

