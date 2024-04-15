from pyspark.sql import SparkSession
import pyspark.sql.functions as functions

spark = SparkSession.builder.master("spark://3e410cc871ef:7077").appName("SparkSQL").getOrCreate()

orders = spark.read.option("header", "false").option("inferSchema", "true").csv("/files/customer-orders.csv")
    
print("Here is our inferred schema:")
orders.printSchema()

print("Total sum of orders")
(
    orders
    .withColumnRenamed("_c0", "customerId").withColumnRenamed("_c1", "productId").withColumnRenamed("_c2", "price")
    .groupBy("customerId")
    .agg(functions.sum("price").alias("total_spent"))
    .withColumn("total_spent", functions.format_number("total_spent", 2)).sort("total_spent", ascending=[False])
    .show()
)

spark.stop()