import findspark
findspark.init()
from pyspark import SparkConf , SparkContext
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

conf = SparkConf().setAppName("TotalStudent")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd1 = sc.textFile("C:\\Users\\abhishek.srivastava\\vscode\\work\\Spark\\Section-3\\Mini-Project\\StudentData.csv")
header=rdd1.first()
# step to remove first row which is header
rdd2 = rdd1.filter(lambda x: x != header)
os.system('cls||system')

rdd3 = rdd2.map(lambda x : (x.split(',')[3],(int(x.split(',')[5]),1)))
#print(rdd3.collect())
# Identify Students having number greater than 50
rdd4 = rdd3.reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1] ))

#rdd5 = rdd4.map(lambda x: round(x[1]))

print("Total number per course and total students : \n {0} \n".format(rdd4.collect()))

rdd5 = rdd4.map(lambda x: (x[0],round(x[1][0]/x[1][1])))

print("Average per course : \n {0} \n".format(rdd5.collect()))











#print("Total number of Students : \n {0} \n".format(rdd2.count()))

