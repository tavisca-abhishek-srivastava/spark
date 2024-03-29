from pyspark import SparkConf , SparkContext

###########################
# this part is for python path
import findspark
import os
import sys
findspark.init()
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
###########################
conf = SparkConf().setAppName("Map lambda")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("./abc.txt")
rdd2 = text.map(lambda x: x + " abhishek") # here input to map (x) is string and return type is list. Lambda is adding "abhishek" in every string passed to it
print(rdd2.collect())