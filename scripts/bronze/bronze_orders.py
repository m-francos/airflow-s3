from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BronzeOrders").getOrCreate()

df = spark.read.json("/home/maite/lakehouse/landing/orders.json")

df.show()

df.write.mode("overwrite").parquet("/home/maite/lakehouse/bronze/orders.parquet")

spark.stop()
