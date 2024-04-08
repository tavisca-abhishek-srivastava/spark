from pyspark import SparkConf , SparkContext
import os

conf = SparkConf().setAppName("Total-Students")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("./StudentData.csv")

first_row = rdd.first()


print(rdd.collect())
print(first_row)
rdd1 = rdd.filter
print(rdd1.collect())

print("Total number of students {0}".format(rdd1.count()))
