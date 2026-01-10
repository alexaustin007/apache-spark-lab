from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time


spark = SparkSession.builder.appName("SparkCaching").getOrCreate()

print("without caching")

df = spark.read.csv("routes_enriched.csv", header=True, inferSchema=True)

print(f"Total rows: {df.count()}")
print(f"Schema:")
df.printSchema()



# Query1 - Count records by origin airport

start_time = time.time()
query1 = df.groupBy("origin_airport").count().orderBy(col("count").desc())
result1 = query1.show(10)
time1 = time.time() - start_time


# Query - Count records by destination airport

start_time = time.time()
query2 = df.groupBy("destination_airport").count().orderBy(col("count").desc())
result2 = query2.show(10)
time2 = time.time() - start_time

# Query3 -  Average stops by airline

start_time = time.time()
query3 = df.groupBy("airline_name").agg(avg("stops").alias("avg_stops")).orderBy(col("avg_stops").desc())
result3 = query3.show(10)
time3 = time.time() - start_time



# input("\nPress Enter after checking Spark UI...")




# PHASE 2: WITH CACHING
print("\n" + "="*80)
print("PHASE 2: WITH CACHING - See the Difference!")
print("="*80)

# Cache in memory
df.cache()

df.count()

print("✅ DataFrame is now cached!")


# Query 1 - Count records by origin airport

start_time = time.time()
query1_cached = df.groupBy("origin_airport").count().orderBy(col("count").desc())
result1_cached = query1_cached.show(10)
time1_cached = time.time() - start_time
print(f"Query 1 (cached) took: {time1_cached:.2f} seconds")

# Query 2 - Count records by destination airport

start_time = time.time()
query2_cached = df.groupBy("destination_airport").count().orderBy(col("count").desc())
result2_cached = query2_cached.show(10)
time2_cached = time.time() - start_time


# Query 3 -  Average stops by airline

start_time = time.time()
query3_cached = df.groupBy("airline_name").agg(avg("stops").alias("avg_stops")).orderBy(col("avg_stops").desc())
result3_cached = query3_cached.show(10)
time3_cached = time.time() - start_time


input("\nPress Enter to exit and stop Spark UI...")