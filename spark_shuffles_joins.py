from pyspark.sql import SparkSession
from pyspark.sql import functions as F



spark = SparkSession.builder.appName('shufflingAndJoining').getOrCreate()

customer_df = spark.read.csv('customers.csv', header = True, inferSchema = True)
orders_df = spark.read.csv('orders.csv', header = True, inferSchema = True)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

joined_df = customer_df.join(orders_df, on='customer_id', how = "inner")
# joined_df.show()

total_amount_spend_customer = joined_df.groupBy("customer_id", "name").agg(F.sum("amount").alias("total_amount_spend_by_customer"),
                                                                           F.count("order_id").alias("order_count")).orderBy(F.desc("total_amount_spend_by_customer"))

total_amount_spend_customer.show()

print("Spark UI is running at: http://localhost:4040")
print("Press Ctrl+C to stop...")
input("Press Enter to stop Spark...")
