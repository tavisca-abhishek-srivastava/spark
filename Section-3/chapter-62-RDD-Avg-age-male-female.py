from pyspark import SparkConf , SparkContext
import os

conf = SparkConf().setAppName("Avg-per-course")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("./StudentData.csv")

header = rdd.first()
rdd1 = rdd.filter(lambda x: x!= header)
rdd2 = rdd1.map(lambda x: (x.split(',')[1] , (int(x.split(',')[0]),1)))
rdd3 = rdd2.reduceByKey(lambda x , y : (x[0]+y[0],x[1]+y[1])) # x is tuple and y is tuple

rdd4 = rdd3.map(lambda x : (x[0] , round(x[1][0]/x[1][1])))


print("Avg age male and female {0}".format(rdd4.collect()))