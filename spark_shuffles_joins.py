from pyspark.sql import SparkSession
from pyspark.sql import functions as F



spark = SparkSession.builder.appName('shufflingAndJoining').getOrCreate()

customer_df = spark.read.csv('customers.csv', header = True, inferSchema = True)
orders_df = spark.read.csv('orders.csv', header = True, inferSchema = True)

joined_df = customer_df.join(orders_df, on='customer_id', how = "inner")
joined_df.show()

print("Spark UI is running at: http://localhost:4040")
print("Press Ctrl+C to stop...")
input("Press Enter to stop Spark...")
