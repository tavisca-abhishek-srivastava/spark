from pyspark import SparkConf , SparkContext
import os

conf = SparkConf().setAppName("Avg-per-course")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("./StudentData.csv")

header = rdd.first()
rdd1 = rdd.filter(lambda x: x!= header)
rdd2 = rdd1.map(lambda x: (x.split(',')[3] , int(x.split(',')[5])))
rdd_max = rdd2.reduceByKey(lambda x , y : (x if x > y else y))
rdd_min = rdd2.reduceByKey(lambda x , y : (x if x < y else y))


print("min mark per course {0}".format(rdd_min.collect()))
print("max mark per course {0}".format(rdd_max.collect()))