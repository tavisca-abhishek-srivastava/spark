from pyspark import SparkConf , SparkContext
import os

conf = SparkConf().setAppName("Total-Students")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("./StudentData.csv")

header = rdd.first()


print(rdd.collect())
print(header)
rdd1 = rdd.filter(lambda x: x!= header)

rdd2 = rdd.filter(lambda x: x.split(',')[1] == "Male")
rdd3 = rdd.filter(lambda x: x.split(',')[1] == "Female")

rdd2_male_numbers = rdd2.map( lambda x : (x.split(',')[1] , x.split(',')[5])

rdd3_female_numbers = rdd3.map( lambda x : (x.split(',')[1] , x.split(',')[5])

print(rdd2_male_numbers)