from pyspark import SparkConf , SparkContext
import os

conf = SparkConf().setAppName("failed-passed-students")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("./StudentData.csv")

header = rdd.first()
# print(rdd.collect())
# print(header)
rdd1 = rdd.filter(lambda x: x!= header)

rdd2 = rdd.filter(lambda x: int(x.split(',')[5]) >= 33)


print("Total number of male students {0}".format(rdd2.count()))
