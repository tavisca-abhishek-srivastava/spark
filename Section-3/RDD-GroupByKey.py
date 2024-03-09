from pyspark import SparkConf , SparkContext
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


conf = SparkConf().setAppName("GroupByKey")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("c:\\tmp\\rdd-disrtinct.txt")
rdd2 = text.flatMap(lambda x: x.split(' '))
#print("Flat Mapped Data: \n {0} ".format(rdd2.collect()))
rdd3 = rdd2.map(lambda x: (x,len(x)+ random.randint(1,99)) )
print(rdd3.groupByKey().mapValues(list).collect())






