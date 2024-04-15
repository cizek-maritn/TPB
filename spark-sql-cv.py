from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.master("spark://3e410cc871ef:7077").appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()

print("Average number of friends by age:")
people.groupBy("age").avg("friends").sort("age", ascending=[True]).show()

spark.stop()