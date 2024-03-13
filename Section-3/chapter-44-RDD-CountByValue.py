from pyspark import SparkConf , SparkContext
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


conf = SparkConf().setAppName("countbyValue")
sc = SparkContext(master='local').getOrCreate(conf=conf)
rdd = sc.textFile("./abc-44.txt")
rdd2 = text.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.filter(lambda x: len(x) != 0)



print(rdd3.collect())
print("Total Number of Occurance of Words : \n {0} ".format(rdd3.countByValue()))






