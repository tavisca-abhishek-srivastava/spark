from pyspark import SparkConf , SparkContext
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


conf = SparkConf().setAppName("FlatMap")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("./quiz.txt")
flatmappedrdd = text.flatMap(lambda x: x.split(' '))
rdd2 = flatmappedrdd.filter(lambda x: not(x.startswith('a')) )
print("Mapped Data: \n {0} ".format(rdd2.collect()))






