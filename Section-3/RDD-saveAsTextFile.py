from pyspark import SparkConf , SparkContext
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


conf = SparkConf().setAppName("SaveAsTextFile")
sc = SparkContext(master='local').getOrCreate(conf=conf)
rdd = sc.textFile("c:\\tmp\\rdd-distinct.txt")
rdd.repartition(5)
print(rdd.getNumPartitions())
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,len(x)))
rdd3.saveAsTextFile("c:\\tmp\\saveastextfile\\abc1")
print(rdd3.getNumPartitions())










