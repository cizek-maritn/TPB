from pyspark.sql import SparkSession
import pyspark.sql.functions as functions
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, FloatType
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
import sys
import codecs

spark = SparkSession.builder.appName("ALSExample").getOrCreate()

houses = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/realestate.csv")

train, test = houses.randomSplit([0.9,0.1])

assembler=VectorAssembler().setInputCols(["HouseAge", "DistanceToMRT", "NumberConvenienceStores"]).setOutputCol("features")

train_a=assembler.transform(train)
train_b=train_a.select("features", train_a.PriceOfUnitArea.alias("label"))

test_a=assembler.transform(test)
test_b=test_a.select("features", test_a.PriceOfUnitArea.alias("label"))

dt = DecisionTreeRegressor()
model = dt.fit(train_b)
test_dt = model.transform(test_b)
test_dt.select("label", "prediction").show(truncate=False)

spark.stop()