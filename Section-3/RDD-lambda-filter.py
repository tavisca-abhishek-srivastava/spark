from pyspark import SparkConf , SparkContext
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


conf = SparkConf().setAppName("FlatMap")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("c:\\tmp\\quiz.txt")
rdd2 = text.filter(lambda x: x != 'Great')
print("Mapped Data: \n {0} ".format(rdd2.collect()))






