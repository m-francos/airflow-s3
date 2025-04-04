from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BronzeCustomers").getOrCreate()

df = spark.read.json("/home/maite/lakehouse/landing/customers.json")

df.show()

df.write.mode("overwrite").parquet("/home/maite/lakehouse/bronze/customers.parquet")

spark.stop()

