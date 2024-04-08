from pyspark import SparkConf , SparkContext
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


conf = SparkConf().setAppName("SaveAsTextFile")
sc = SparkContext(master='local').getOrCreate(conf=conf)
rdd = sc.textFile("./abc-45.txt")
rdd.repartition(5)
print("Number of partitions {0} ".format(rdd.getNumPartitions()))
rdd2 = rdd.flatMap(lambda x: x.split(' '))
rdd3 = rdd2.map(lambda x: (x,len(x)))
rdd3.saveAsTextFile("./saveastextfile/abc1") #(by default there will be 2 files inside abc1 as sprak uses 2 partition to process the data and these 2 partitions will save data in 2 files)
print(rdd3.getNumPartitions())








