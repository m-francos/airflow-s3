import os
from pyspark.sql import SparkSession

base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

spark = SparkSession.builder.appName("BronzeCustomers").getOrCreate()

input_path = os.path.join(base_path, "data", "landing", "customers.json")
df = spark.read.json(input_path)

df.show()

output_path = os.path.join(base_path, "data", "bronze", "customers.parquet")
df.write.mode("overwrite").parquet(output_path)

spark.stop()

