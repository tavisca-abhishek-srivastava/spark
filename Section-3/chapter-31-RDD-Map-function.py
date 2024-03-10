from pyspark import SparkConf , SparkContext
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def foo(x):
    list_of_char =  x.split(' ')
    list_of_int = []
    for i in list_of_char:
        list_of_int.append(int(i) + 4)
    
    return list_of_int

conf = SparkConf().setAppName("Map Function")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("/tmp/abc-int.txt")
rdd2 = text.map(foo)   # here input to map (x) is string(containing numbers) and return type is list of lists. here foo is adding 4 in each element of list passed to it
print((rdd2.collect()))