from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkPartitioning").getOrCreate()

df = spark.read.csv("routes_enriched.csv", inferSchema = True, header = True)

# df.show(5)
# print(df.count())
print(df.rdd.getNumPartitions())
print(f"Total rows: {df.count()}")
input("Press Enter to stop...")
