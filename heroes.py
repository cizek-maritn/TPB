from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("/files/marvel-names.txt")

lines = spark.read.text("/files/marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = (
    lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) 
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) 
    .groupBy("id").agg(func.sum("connections").alias("connections"))
)

minConnections = (
    connections.where("connections > 0").agg(func.min("connections")).collect()[0][0]
)

obscureHeroes = (
    connections
    .join(names, connections.id == names.id)
    .where("connections = '{}'".format(minConnections))
    .sort("name", ascending=[True]).show()
)
#mostPopular = connections.sort(func.col("connections").desc()).first()

#mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

#print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")
#print(str(minConnections))

spark.stop()



