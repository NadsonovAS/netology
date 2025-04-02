from pyspark.sql import SparkSession as sc
from pyspark.sql.functions import lag, round, sum
from pyspark.sql.window import Window

spark = sc.builder.getOrCreate()

df = spark.read.csv("./covid.csv", header=True)

print("===============================")
print(
    "Задание 1: Выберите 15 стран с наибольшим процентом переболевших на 31 марта (в выходящем датасете необходимы колонки: iso_code, страна, процент переболевших)"
)
print("===============================")

df_res_1 = df.select(
    df.iso_code,
    df.location,
    # df.date,
    # df.population,
    # df.total_cases,
    round((df.total_cases / df.population * 100), 3).alias("percent"),
).where(df.date == "2021-03-31")


df_res_1 = df_res_1.orderBy(df_res_1.percent.desc())
df_res_1.show(15)

print("===============================")
print(
    "Задание 2: Top 10 стран с максимальным зафиксированным кол=вом новых случаев за последнюю неделю марта 2021 в отсортированном порядке по убыванию (в выходящем датасете необходимы колонки: число, страна, кол=во новых случаев)"
)
print("===============================")

df_res_2 = df.select(df.date, df.location, df.new_cases).where(
    (df.date <= "2021-03-31")
    & (df.date >= "2021-03-25")
    & (df.continent != "NULL")  # убирает вывод континентов из df.location
)


window = Window.partitionBy(df_res_2.location).orderBy(df_res_2.date)


df_res_2 = df_res_2.withColumn("sum_new_cases", sum(df_res_2.new_cases).over(window))


df_res_2 = (
    df_res_2.select(df_res_2.date, df_res_2.location, df_res_2.sum_new_cases)
    .where(df_res_2.date == "2021-03-31")
    .orderBy(df_res_2.sum_new_cases.desc())
)
df_res_2.show(10)


print("===============================")
print(
    "Задание 3: Посчитайте изменение случаев относительно предыдущего дня в России за последнюю неделю марта 2021. (например: в россии вчера было 9150 , сегодня 8763, итог: -387) (в выходящем датасете необходимы колонки: число, кол-во новых случаев вчера, кол-во новых случаев сегодня, дельта)"
)
print("===============================")

df_res_3 = df.select(df.date, df.location, df.new_cases.alias("new_cases_today")).where(
    (df.location == "Russia") & (df.date.between("2021-03-24", "2021-03-31"))
)

window = Window.partitionBy(df_res_3.location).orderBy(df_res_3.date)

df_res_3 = df_res_3.withColumn("prev_new_cases", lag("new_cases_today", 1).over(window))

df_res_3 = df_res_3.select(
    "*", (df_res_3.new_cases_today - df_res_3.prev_new_cases).alias("delta")
)

df_res_3.where(df.date >= "2021-03-25").show()
