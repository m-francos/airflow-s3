from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BronzeOrderItems").getOrCreate()

df = spark.read.json("/home/maite/lakehouse/landing/order_items.json")

df.show()

df.write.mode("overwrite").parquet("/home/maite/lakehouse/bronze/order_items.parquet")

spark.stop()
