from pyspark import SparkConf , SparkContext
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


conf = SparkConf().setAppName("GroupByKey")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("./abc-41.txt")
rdd2 = text.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.filter(lambda x: len(x) != 0)

#print("Flat Mapped Data: \n {0} ".format(rdd2.collect()))
# Identify occurance of any word
dict = {}
rdd4 = rdd3.map(lambda x: (x,1) )
#print(rdd3.collect())
# print("Word and It's Occurance : \n {0} ".format(rdd4.reduceByKey(lambda x,y: (x+y)).collect()))
rdd_reducebykey = rdd4.reduceByKey(lambda x,y: (x+y)) # x and y are value from tuple
print("Word and It's Occurance : \n {0} ".format(rdd_reducebykey))






