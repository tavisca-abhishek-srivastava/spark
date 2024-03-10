from pyspark import SparkConf , SparkContext
import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

def filter_word_with_a(word_to_check):
    if not(x.startswith('a')):
            return True




conf = SparkConf().setAppName("FlatMap")
sc = SparkContext(master='local').getOrCreate(conf=conf)
text = sc.textFile("./quiz.txt")
flatmappedrdd = text.flatMap(lambda x: x.split(' ')) # input to both map and flatmap is list and return type is list [map -> lsit of lists , flatmap -> List of int/string]
rdd2 = flatmappedrdd.filter(filter_word_with_a )
print("Mapped Data: \n {0} ".format(rdd2.collect()))






