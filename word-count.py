from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql.session import SparkSession
import re
import os
import tempfile
import json

conf = SparkConf().setMaster("spark://3e410cc871ef:7077").setAppName("WordCount")
# conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/files/book.txt")
words = input.flatMap(lambda x: re.sub("[.?!,-_*;()&+#$@<>\"]", "",x.lower()).split())
wordCounts = words.countByValue()
sortedDict = {k: v for k, v in reversed(sorted(wordCounts.items(), key=lambda item: item[1]))}
keys = list(sortedDict.keys())
vals = list(sortedDict.values())

for i in range(20):
    cleanWord = keys[i].encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(vals[i]))

print("BONUS")

rdd = sc.textFile("/files/test.json")
input = rdd.map(lambda x: x.strip()).collect()
contentlist = []
for li in input:
    try:
        if str(li[3])=="n":
            var = str(li[11:])
            var=var.replace("\\n", " ")
            contentlist.append(var)
    except:
        pass
#print(str(contentlist))
arts = sc.parallelize(contentlist)
words = arts.flatMap(lambda x: re.sub("[.?!,-_*;()\\&+#$@<>\"\n]", "",x.lower()).split())
wordCounts = words.countByValue()
sortedDict = {k: v for k, v in reversed(sorted(wordCounts.items(), key=lambda item: item[1]))}
keys = list(sortedDict.keys())
vals = list(sortedDict.values())

counter = 0
#print(str(len((keys))))
for i in range(500):
    cleanWord = keys[i].encode()
    if (cleanWord):
        temp=cleanWord.decode()
        if len(temp)>5:
            print(cleanWord.decode() + " " + str(vals[i]))
            counter+=1
    if (counter==20):
        break