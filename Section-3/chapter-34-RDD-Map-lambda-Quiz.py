from pyspark import SparkConf , SparkContext
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

'''def foo(x):
    list_of_words =  x.split(' ')
    list_of_length = []
    for i in list_of_words:
        list_of_length.append(len(i))
    
    return list_of_length'''



conf = SparkConf().setAppName("Quiz")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("c:\\tmp\\quiz.txt")
rdd2 = text.map(lambda x: [len(s) for s in x.split(' ')])
print((rdd2.collect()))




