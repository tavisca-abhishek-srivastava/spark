from pyspark import SparkConf , SparkContext
import os

conf = SparkConf().setAppName("Avg-per-course")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("./StudentData.csv")

header = rdd.first()
rdd1 = rdd.filter(lambda x: x!= header)
rdd2 = rdd1.map(lambda x: (x.split(',')[3] , (int(x.split(',')[5]),1)))
rdd3 = rdd2.reduceByKey(lambda x , y : (x[0]+y[0],x[1]+y[1])) # x is tuple and y is tuple

rdd4 = rdd3.map(lambda x : (x[0] , x[1][0]/x[1][1]))


print("Avg mark per course {0}".format(rdd4.collect()))