from pyspark import SparkConf , SparkContext
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


conf = SparkConf().setAppName("FlatMap")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("./abc-36.txt")
rdd2 = text.filter(lambda x: x != 'Great') # if condition in lambda returns true for the item passed to it , then that item will be added in final output.
print("Mapped Data: \n {0} ".format(rdd2.collect()))






