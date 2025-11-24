from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sparkMastery").getOrCreate()


df = spark.read.csv('routes_enriched.csv', header=True, inferSchema=True)

# print(df.show(5))

# df.printSchema()
# print(df.count())


distance_above_5000 = df.filter(df.distance_km > 5000).groupBy("airline_name").count().orderBy("count", ascending=False).limit(10)
distance_above_5000.show()

spark.stop()


