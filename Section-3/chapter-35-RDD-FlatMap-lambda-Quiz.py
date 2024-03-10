from pyspark import SparkConf , SparkContext
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

'''def foo(x):
    list_of_words =  x.split(' ')
    list_of_length = []
    for i in list_of_words:
        list_of_length.append(len(i))
    
    return list_of_length'''



conf = SparkConf().setAppName("FlatMap")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("/tmp/Quiz.txt")

print(text.collect())

rdd2 = text.map(lambda x: [s for s in x.split(' ')]) # input to both map and flatmap is list and return type is list [map -> lsit of lists , flatmap -> List of int/string]
print("Mapped Data: \n {0} ".format(rdd2.collect()))

FlatMaprdd2 = text.flatMap(lambda x: [s for s in x.split(' ')]) # input to both map and flatmap is list and return type is list [map -> lsit of lists , flatmap -> List of int/string]
print("Flat Mapped Data: \n {0} ".format((FlatMaprdd2.collect())))




