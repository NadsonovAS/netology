import requests
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, IntegerType, StringType

# Получение схем из Schema Registry
registry_url = "http://localhost:8081"
sr_users_value = "users-value"
sr_users_value_get = requests.get(
    f"{registry_url}/subjects/{sr_users_value}/versions/latest"
)
schema_users = sr_users_value_get.json()["schema"]

# Запуск spark сессии
spark = (
    SparkSession.builder.appName("SparkStreamingKafka")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.spark:spark-avro_2.12:3.5.5",
    )
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

# Сырые данные из Kafka
input_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "users") \
  .load()

# Десериализация Avro и извлечение полей таблицы users
df_users = input_stream.select(
    col("timestamp"),
    from_avro(expr("substring(value, 6)"), schema_users).alias("users"),
).select(
    col("timestamp"),
    col("users.userid").alias("userid"),
    col("users.gender").alias("gender"),
)


# Создание статичной таблицы
users_data = [('User_1',"Jimmy",18),('User_2',"Hank",48),('User_3',"Johnny",9),('User_4',"Erle",40), ('User_5',"Lolly",81),('User_6',"Rozetta",84),('User_7',"Timmy",19),('User_8',"Bob",15)]
users_schema = StructType().add("user_id",StringType()).add("user_name", StringType()).add("user_age", IntegerType())
users = spark.createDataFrame(data=users_data,schema=users_schema)


# join stream + static
join_stream = df_users.join(users, users.user_id == df_users.userid, "left_outer").select(col("timestamp"), col("userid"), col("gender"), col("user_name"), col("user_age"))

# Вывод в косноль
join_stream.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()




