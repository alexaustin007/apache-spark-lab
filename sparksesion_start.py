from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("sparkMastery").getOrCreate()


df = spark.read.csv('routes_enriched.csv', header=True, inferSchema=True)



df.show(5)

df.printSchema()
print(df.count())


distance_above_5000 = df.filter(df.distance_km > 5000).groupBy("airline_name").count().orderBy("count", ascending=False).limit(10)
distance_above_5000.show()


distance_above_8000 = df.filter((df.distance_km > 8000) & (df.stops == 0) & (df.origin_country == 'India')).groupBy("airline_name").count().orderBy("count", ascending=False).limit(10)
distance_above_8000.show()

multiple_agg = df.groupBy("airline_name").agg(F.count('*').alias("total_flights"),
                                              F.avg("distance_km").alias("avg_distance"),
                                              F.max("seats").alias("max_seats")).orderBy("total_flights", ascending=False).limit(10)
multiple_agg.show()

contains_new = df.filter(df.origin_city.like("%New%")).groupBy("origin_city").count().orderBy("count", ascending=False)

contains_new.show()

window_spec = Window.partitionBy("airline_name").orderBy(F.desc("distance_km"))

df_ranked = df.withColumn("rank", F.row_number().over(window_spec) )
# df_ranked.show(5)

df_ranked.select("airline_name", "origin_city", "destination_city", "distance_km", "rank").filter((df_ranked.rank <= 3) & (df_ranked.airline_name.isin(['Ryanair','American Airlines','United Airlines']))).show()

spark.stop()


