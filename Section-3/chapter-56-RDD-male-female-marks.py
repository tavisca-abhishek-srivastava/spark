from pyspark import SparkConf , SparkContext
import os

conf = SparkConf().setAppName("male-female-marks")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("./StudentData.csv")

header = rdd.first()


print(rdd.collect())
print(header)
rdd1 = rdd.filter(lambda x: x!= header)

rdd2 = rdd.filter(lambda x: x.split(',')[1] == "Male")
rdd3 = rdd.filter(lambda x: x.split(',')[1] == "Female")

rdd2_male_numbers = rdd2.map( lambda x : (x.split(',')[1] , int(x.split(',')[5])))

rdd3_female_numbers = rdd3.map( lambda x : (x.split(',')[1] , int(x.split(',')[5])))

total_marks_male_rdd = rdd2_male_numbers.reduceByKey(lambda x , y: (int(x)+int(y)))

print(total_marks_male_rdd.collect())

### or simple solution

rdd2_map_gender_number = rdd1.map( lambda x : (x.split(',')[1] , int(x.split(',')[5])))
total_marks_male_female_rdd = rdd2_map_gender_number.reduceByKey(lambda x , y: (int(x)+int(y)))
print(total_marks_male_female_rdd.collect())