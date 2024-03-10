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
text = sc.textFile("./abc.txt")

print(text.collect())  # list of string(lines of file)

rdd2 = text.map(lambda x: [s+'z' for s in x.split(' ')]) # input to map is list of string(after addition of z in it) and return type is  list of lists. input to lambda is string(lines from file) and output is list of words
print("Mapped Data: \n {0} ".format(rdd2.collect()))

FlatMaprdd2 = text.flatMap(lambda x: [s+'z' for s in x.split(' ')]) # input to flatmap is list of string(after addition of z in it) and return type is list. input to lambda is string(lines from file) and output is list of words
print("Flat Mapped Data: \n {0} ".format((FlatMaprdd2.collect())))




