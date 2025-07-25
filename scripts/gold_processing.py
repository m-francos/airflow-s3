import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct
from dotenv import load_dotenv

def main():
    load_dotenv()

    spark = SparkSession.builder \
        .appName("GoldLayerProcessing") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

    try:
        silver_path = "s3a://mf-atividadebucket/silver/"
        gold_path = "s3a://mf-atividadebucket/gold/gold_dataset.parquet"

        customers = spark.read.parquet(f"{silver_path}customers.parquet")
        orders = spark.read.parquet(f"{silver_path}orders.parquet")
        order_items = spark.read.parquet(f"{silver_path}order_items.parquet")

        print(f"Total de pedidos: {orders.count()}")
        print(f"Total de itens: {order_items.count()}")

        orders_with_customers = orders.join(
            customers,
            orders["customer_id"] == customers["id"],
            "left"
        ).select(
            orders["id"].alias("order_id"),
            orders["customer_id"],
            orders["date"],
            orders["status"],
            customers["city"],
            customers["state"]
        )

        order_totals = order_items.groupBy("order_id").agg(
            sum("order_item_subtotal").alias("order_total")
        )

        final_data = orders_with_customers.join(
            order_totals,
            orders_with_customers["order_id"] == order_totals["order_id"],
            "left"
        ).select(
            orders_with_customers["order_id"],
            orders_with_customers["city"],
            orders_with_customers["state"],
            order_totals["order_total"]
        )

        gold_data = final_data.groupBy("city", "state").agg(
            countDistinct(col("order_id")).alias("quantidade_pedidos"),
            sum(col("order_total")).alias("valor_total_pedidos")
        )

        total_gold = gold_data.agg(sum("quantidade_pedidos")).collect()[0][0]
        print(f"Total de pedidos na Gold: {total_gold} (deve bater com {orders.count()})")

        gold_data.write.mode("overwrite").parquet(gold_path)
        print(f"Dados gold salvos em: {gold_path}")

    except Exception as e:
        print(f"Erro: {str(e)}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
