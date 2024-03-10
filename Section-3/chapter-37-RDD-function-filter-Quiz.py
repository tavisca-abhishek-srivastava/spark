from pyspark import SparkConf , SparkContext
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

def filter_word_with_a(word_to_check):
    if not(word_to_check.startswith('a') or word_to_check.startswith('c') ):
            return True




conf = SparkConf().setAppName("FlatMap")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("./abc-37.txt")
flatmappedrdd = text.flatMap(lambda x: x.split(' ')) #  x.split(' ') -> list 
rdd2 = flatmappedrdd.filter(filter_word_with_a ) # every item from flatmappedrdd list will be passed to "filter_word_with_a" function
print("Mapped Data: \n {0} ".format(rdd2.collect()))






