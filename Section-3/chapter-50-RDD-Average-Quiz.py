from pyspark import SparkConf , SparkContext
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

conf = SparkConf().setAppName("Movie Rating")
sc = SparkContext(master='local').getOrCreate(conf=conf)
rdd = sc.textFile("./movie_ratings.csv")
rdd1 = rdd.map(lambda x: (x.split(',')[0],(int(x.split(',')[1]),1)))
print(rdd1.collect())
print("Movie, It's total review Points and Number of Reviews : \n {0} ".format(rdd1.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).collect()))

rdd2 = rdd1.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
rdd3 = rdd2.map(lambda x: (x[0],round(x[1][0]/x[1][1])))

print("Movie and It's Average : \n {0} ".format(rdd3.collect()))