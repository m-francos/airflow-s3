from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Silver_Orders").getOrCreate()

df = spark.read.parquet("/home/maite/lakehouse/bronze/orders.parquet")

df_silver = df.withColumnRenamed("order_id", "id") \
              .withColumnRenamed("order_date", "date") \
              .withColumnRenamed("order_customer_id", "customer_id") \
              .withColumnRenamed("order_status", "status")

df_silver.write.mode("overwrite").parquet("/home/maite/lakehouse/silver/orders.parquet")

spark.stop()
