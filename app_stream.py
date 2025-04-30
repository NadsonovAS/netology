import requests
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, sum, window, avg, to_json, struct

# Получение схем из Schema Registry
registry_url = "http://localhost:8081"
sr_purchases_value = "purchases-value"
sr_products_value = "products-value"

sr_purchases_value_get = requests.get(
    f"{registry_url}/subjects/{sr_purchases_value}/versions/latest"
)
sr_products_value_get = requests.get(
    f"{registry_url}/subjects/{sr_products_value}/versions/latest"
)

schema_purchases = sr_purchases_value_get.json()["schema"]
schema_products = sr_products_value_get.json()["schema"]

# Сессия
spark = (
    SparkSession.builder.appName("SparkStreamingKafka")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.spark:spark-avro_2.12:3.5.5",
    )
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

# Чтение сырых данных из Kafka
purchases_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "purchases")
    .load()
)

products_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "products")
    .load()
)

# Десериализация Avro и извлечение полей

# Таблица purchases
df_purchases = purchases_stream.select(
    col("timestamp"),
    from_avro(expr("substring(value, 6)"), schema_purchases).alias("purchases"),
).select(
    col("timestamp").alias("purchases_ts"),
    col("purchases.productid").alias("productid"),
    col("purchases.quantity").alias("quantity")
)

# Таблица products
df_products = products_stream.select(
    col("timestamp"),
    from_avro(expr("substring(value, 6)"), schema_products).alias("products"),
).select(
    col("timestamp").alias("products_ts"),
    col("products.id").alias("id"),
    col("products.price").alias("price")
)

# Добавление метки и агрегация для purchases
df_purchases_window = df_purchases.withWatermark("purchases_ts", "1 seconds") \
    .groupBy(
    window(col("purchases_ts"), "1 minute").alias("purchases_ts"),
    col("productid")
).agg(
    sum("quantity").alias("total_quantity")
)

# Добавление метки и агрегация для products
df_products_window = df_products.withWatermark("products_ts", "1 seconds") \
    .groupBy(
    window(col("products_ts"), "1 minute").alias("products_ts"),
    col("id")
).agg(
    avg("price").alias("price")
)

# Join двух таблиц с подсчетом и фильтром
res = df_purchases_window.join(
    df_products_window,
    (df_purchases_window.productid == df_products_window.id) &
    (df_purchases_window.purchases_ts == df_products_window.products_ts),
    "inner"
).select(col("purchases_ts"), col("productid"), col("total_quantity"), col("price"),
         (col("total_quantity") * col("price")).alias("total_price")).filter("total_price > 2000")

res_kafka = res.select(
    col("productid").cast("string").alias("key"),
    to_json(struct("productid", "total_price", "purchases_ts", "total_quantity", "price")).alias("value")
)

# Запись в Kafka
res_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alert-purchases") \
    .option("checkpointLocation", "./tmp/checkpoints/alert-purchases") \
    .outputMode("append") \
    .option("truncate", False) \
    .trigger(processingTime="60 seconds") \
    .start() \
    .awaitTermination()
