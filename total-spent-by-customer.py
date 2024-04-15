from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setMaster("spark://3e410cc871ef:7077").setAppName("MinTemperatures")
# conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = float(fields[2])
    return (stationID, entryType)

lines = sc.textFile("/files/customer-orders.csv")
parsedLines = lines.map(parseLine)

prices = parsedLines.reduceByKey(add)
results = prices.sortBy(lambda x: x[1], False).collect()

for result in results:
    print("id: "+result[0] + " total price: "+str(result[1]))
