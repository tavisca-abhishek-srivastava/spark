from pyspark import SparkConf , SparkContext
import os

conf = SparkConf().setAppName("Enrollment-per-course")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("./StudentData.csv")

header = rdd.first()


print(rdd.collect())
print(header)
rdd1 = rdd.filter(lambda x: x!= header)
rdd2 = rdd1.map(lambda x: (x.split(',')[3] , 1))
rdd3 = rdd2.reduceByKey(lambda x , y : x+y)

print("enrollment per course {0}".format(rdd3.collect()))