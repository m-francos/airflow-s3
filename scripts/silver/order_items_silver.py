from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Silver_OrderItems").getOrCreate()

df = spark.read.parquet("/home/maite/lakehouse/bronze/order_items.parquet")

df_silver = df.withColumnRenamed("order_item_id", "id") \
              .withColumnRenamed("order_item_order_id", "order_id") \
              .withColumnRenamed("order_item_product_id", "product_id") \
              .withColumnRenamed("order_item_quantity", "quantity") \
              .withColumnRenamed("order_item_product_price", "price")

df_silver.write.mode("overwrite").parquet("/home/maite/lakehouse/silver/order_items.parquet")

spark.stop()
