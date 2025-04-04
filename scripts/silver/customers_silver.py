from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Silver_Customers").getOrCreate()

df = spark.read.parquet("/home/maite/lakehouse/bronze/customers.parquet")

df_silver = df.withColumnRenamed("customer_id", "id") \
              .withColumnRenamed("customer_fname", "first_name") \
              .withColumnRenamed("customer_lname", "last_name") \
              .withColumnRenamed("customer_email", "email") \
              .withColumnRenamed("customer_city", "city") \
              .withColumnRenamed("customer_state", "state")

df_silver.write.mode("overwrite").parquet("/home/maite/lakehouse/silver/customers.parquet")

spark.stop()
