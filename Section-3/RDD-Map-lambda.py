from pyspark import SparkConf , SparkContext
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
conf = SparkConf().setAppName("Map lambda")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("c:\\tmp\\abc.txt")
rdd2 = text.map(lambda x: x + " abhishek")
print(rdd2.collect())




