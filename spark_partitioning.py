from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SparkPartitioning").getOrCreate()

# df = spark.read.csv("routes_enriched.csv", inferSchema = True, header = True)

# df.show(5)
# print(df.count())
# print(df.rdd.getNumPartitions())
# print(f"Total rows: {df.count()}")
# repartitioned_df = df.repartition(20)
# print(f"Partitions after repartition(20): {repartitioned_df.rdd.getNumPartitions()}")

  # Trigger action and check UI
# repartitioned_df.count()

# coalesced_df = repartitioned_df.coalesce(5)
# print(coalesced_df.rdd.getNumPartitions())
# coalesced_df.count()

df = spark.read.csv("orders.csv", inferSchema = True, header = True)
# df.show(5)

df_year = df.withColumn("year", year(df["order_date"]))
# df_year.show(5)
df_year.write.mode("overwrite").partitionBy("year").parquet("orders_partitioned/")

# df_year_r = spark.read.parquet("orders_partitioned/")
# df_year_r.show(5)
df_filtered = spark.read.parquet("orders_partitioned/").filter(col("year") == 2024)
df_filtered.count()  s

df_no_pruning = spark.read.parquet("orders_partitioned/").filter(col("customer_id") == 1)
df_no_pruning.count()
input("Press Enter to stop...")
