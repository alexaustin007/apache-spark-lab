from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkPartitioning").getOrCreate()

df = spark.read.csv("routes_enriched.csv", inferSchema = True, header = True)

# df.show(5)
# print(df.count())
# print(df.rdd.getNumPartitions())
# print(f"Total rows: {df.count()}")
repartitioned_df = df.repartition(20)
# print(f"Partitions after repartition(20): {repartitioned_df.rdd.getNumPartitions()}")

  # Trigger action and check UI
# repartitioned_df.count()

coalesced_df = repartitioned_df.coalesce(5)
print(coalesced_df.rdd.getNumPartitions())
coalesced_df.count()

input("Press Enter to stop...")
