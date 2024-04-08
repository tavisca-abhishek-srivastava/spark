from pyspark import SparkConf , SparkContext
import os

conf = SparkConf().setAppName("Total-Students")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("./StudentData.csv")

print(rdd.collect())