from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


spark = SparkSession.builder.appName("debugSparkJobs").getOrCreate()


  # Orders: customer_id as STRING (explicit)
orders_schema = StructType([
      StructField("order_id", StringType(), True),
      StructField("customer_id", StringType(), True),  # STRING
      StructField("amount", DoubleType(), True)
])

orders_data = [
      ("1001", "1", 250.50),
      ("1002", "2", 763.23),
      ("1003", "3", 927.72)
]

  # Customers: customer_id as INTEGER (explicit)
customers_schema = StructType([
      StructField("customer_id", IntegerType(), True),  # INTEGER
      StructField("name", StringType(), True)
])

customers_data = [
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
]

order_df = spark.createDataFrame(orders_data, orders_schema)
customer_df = spark.createDataFrame(customers_data, customers_schema)

  # Check schemas first
order_df.printSchema()
customer_df.printSchema()

  # This should now fail or give unexpected results
joined_df = order_df.join(customer_df, "customer_id", "inner")
# joined_df.show()

broken_df = order_df.select("order_id", "customer_id", "amount")
# broken_df.show()

null_data = [
      ("1001", 1, 250.50),
      ("1002", 2, None),      # NULL amount
      ("1003", 3, 340.75),
      ("1004", 4, None),      # NULL amount
      ("1005", 5, 120.00)
]

null_schema = ["order_id", "customer_id", "amount"]
null_df = spark.createDataFrame(null_data, null_schema)
# null_df.show()

discounted = null_df.withColumn("discount", col("amount")*0.1)
discounted.show()

result = null_df.withColumn("amount_doubled", col("amount") * 2)
result.show()
total_rows = null_df.count()
print(total_rows)


non_null_amounts = null_df.filter(col("amount").isNotNull()).count()

total_amount = null_df.agg(sum("amount")).collect()[0][0]
print(f"Total amount: {total_amount}")



avg_amount = null_df.agg(avg("amount")).collect()[0][0]
print(f"Average amount: {avg_amount}")


div_data = [
      ("1001", 1, 100.0),
      ("1002", 0, 200.0),    # customer_id = 0 will cause division by zero!
      ("1003", 2, 300.0),
      ("1004", 0, 400.0),    # Another zero!
      ("1005", 5, 500.0)
]

div_schema = ["order_id", "customer_id", "amount"]
div_df = spark.createDataFrame(div_data, div_schema)

print("Data with some customer_id = 0:")
div_df.show()

  # Try to divide amount by customer_id - this WILL crash!
print("Attempting division by customer_id...")
result = div_df.withColumn("amount_per_customer", col("amount") / col("customer_id"))
result.show()  # This should fail
def risky_categorize(amount):
    """Categorize orders - but crashes on certain amounts!"""
    if amount is None:
        raise ValueError("Amount cannot be None!")  # This will crash!
    elif amount < 100:
        return "small"
    elif amount < 500:
        return "medium"
    else:
        return "large"

  # Register the UDF
categorize_udf = udf(risky_categorize, StringType())

  # Use the data with NULLs from before
print("Attempting to categorize orders (will crash on NULL)...")
categorized = null_df.withColumn("category", categorize_udf(col("amount")))
categorized.show()  # This WILL crash!
input("Press Enter to stop...")