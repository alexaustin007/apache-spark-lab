from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("sparkMastery").getOrCreate()


df = spark.read.csv('routes_enriched.csv', header=True, inferSchema=True)



# print(df.show(5))

# df.printSchema()
# print(df.count())


# distance_above_5000 = df.filter(df.distance_km > 5000).groupBy("airline_name").count().orderBy("count", ascending=False).limit(10)
# distance_above_5000.show()


# distance_above_8000 = df.filter((df.distance_km > 8000) & (df.stops == 0) & (df.origin_country == 'India')).groupBy("airline_name").count().orderBy("count", ascending=False).limit(10)
# distance_above_8000.show()

multiple_agg = df.groupBy("airline_name").agg(F.count('*').alias("total_flights"),
                                              F.avg("distance_km").alias("avg_distance"),
                                              F.max("seats").alias("max_seats")).orderBy("total_flights", ascending=False).limit(10)
multiple_agg.show()

spark.stop()


