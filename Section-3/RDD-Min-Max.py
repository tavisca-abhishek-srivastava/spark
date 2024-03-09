from pyspark import SparkConf , SparkContext
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

conf = SparkConf().setAppName("Movie Rating")
sc = SparkContext(master='local').getOrCreate(conf=conf)
rdd = sc.textFile("c:\\tmp\\movie_ratings.csv")
#convert the input into tuple of movie and review
rdd1 = rdd.map(lambda x: (x.split(',')[0],(int(x.split(',')[1]))))
print(rdd1.collect())
#print("Movie, It's total review Points and Number of Reviews : \n {0} ".format(rdd1.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).collect()))

#for Min
rdd2 = rdd1.reduceByKey(lambda x,y: x if x < y  else y)
print("Lowest Review : \n {0} ".format(rdd2.collect()))

#for Max
rdd3 = rdd1.reduceByKey(lambda x,y: x if x > y  else y)
print("Highest Review : \n {0} ".format(rdd3.collect()))
