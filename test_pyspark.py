from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from time import time
import numpy as np
import os

conf = SparkConf()  # create the configuration
conf.set("spark.jars", "/home/user/Project/postgresql-42.3.5.jar")
conf.setMaster("spark://user-Legion-5-Pro-16ITH6:7077")

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config(conf=conf) \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable", "users") \
    .option("user", "myuser") \
    .option("password", "password") \
    .option("driver", "org.postgresql.Driver") \
    .load()

df.printSchema()

times = []
df.createOrReplaceTempView("users")

for i in range(10):
	start = time()
# Измеряем время выполнения 
	spark.sql("Select name From users Where rating < 5.0;").show(10)
	#сложный запрос, если нужен легкий, то в spark.sql("Select count(*) From users;")
	#cnt = df.count()
	end = time()
	times.append(end-start)

print('Время каждого запуска -',times)
print('Среднее время по 10 проходам -',np.mean(times))
# Select AVG (age) From users WHERE (name='Mark' OR name='Sasha') AND country in (SELECT country FROM (SELECT country, AVG(rating) FROM users GROUP BY country ORDER BY country LIMIT 1) AS country_table);
# Select name From users Where rating < 5.0;
