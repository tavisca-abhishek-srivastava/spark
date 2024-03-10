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



conf = SparkConf().setAppName("Distinct")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("./abc-39.txt")
rdd2 = text.flatMap(lambda x:[ int(s) for s in x.split(' ')])
print("Flat Mapped Data: \n {0} ".format(rdd2.collect()))

distinct_rdd2 = rdd2.distinct()
print("Unique Data: \n {0} ".format((distinct_rdd2.collect())))




