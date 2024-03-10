from pyspark import SparkConf , SparkContext
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


conf = SparkConf().setAppName("FlatMap")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("./abc-36.txt")
rdd2 = text.filter(lambda x: x != 'Great') # if input to filter for any item is true(based upon condition) then that item is added to the output list
print("Mapped Data: \n {0} ".format(rdd2.collect()))






