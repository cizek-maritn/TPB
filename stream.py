from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import lower
from pyspark.sql.functions import regexp_replace
import pyspark.sql.functions as func
import time

spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
lines = lines.withColumn("value", regexp_replace("value", ",", " "))

words = lines.select(explode(split(lines.value, " "))).alias("word")
#words.printSchema()
words = words.withColumn("col", lower(words["col"]))
words = words.withColumn("time", func.current_timestamp())

wordCounts = words.groupBy(func.window(func.col("time"), "30 seconds", "15 seconds"), func.col("col")).count().sort("window","count", ascending=[True,False])

query = wordCounts.writeStream.outputMode("complete").format("console").start()

print("we started")

query.awaitTermination()

spark.stop()

#.sort("count", ascending=[False])