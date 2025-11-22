from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("sparkMastery").getOrCreate()


df = spark.read.csv('routes_enriched.csv', header=True, inferSchema=True)

print(df.show(5))
spark.stop()

