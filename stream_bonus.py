from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import lower
from pyspark.sql.functions import regexp_replace
import pyspark.sql.functions as func
import time


spark = SparkSession.builder.appName("FileStream").getOrCreate()

accessLines = spark.readStream.text("streaming")
#lines = accessLines.withColumn("value", regexp_replace("value", r'[,]', ""))
lines = accessLines.withColumn("value", regexp_replace("value", r'[.,\n]', ""))

words = lines.select(explode(split(lines.value, " "))).alias("word")
#words.printSchema()
words = words.withColumn("col", lower(words["col"]))

wordCounts = words.groupBy("col").count().sort("count", ascending=[False])

query = wordCounts.writeStream.outputMode("complete").format("console").start()

print("we started")

query.awaitTermination()

spark.stop()