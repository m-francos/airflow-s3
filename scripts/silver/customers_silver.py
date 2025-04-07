import os
from pyspark.sql import SparkSession

base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
bronze_path = os.path.join(base_path, "data", "bronze", "customers.parquet")
silver_path = os.path.join(base_path, "data", "silver", "customers.parquet")

spark = SparkSession.builder.appName("Silver_Customers").getOrCreate()

df = spark.read.parquet(bronze_path)

df_silver = df.withColumnRenamed("customer_id", "id") \
              .withColumnRenamed("customer_fname", "first_name") \
              .withColumnRenamed("customer_lname", "last_name") \
              .withColumnRenamed("customer_email", "email") \
              .withColumnRenamed("customer_city", "city") \
              .withColumnRenamed("customer_state", "state")

df_silver.write.mode("overwrite").parquet(silver_path)

spark.stop()

