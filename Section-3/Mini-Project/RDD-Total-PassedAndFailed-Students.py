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

rdd3 = rdd2.map(lambda x : (x.split(',')[1],x.split(',')[5]))
#print(rdd3.collect())
# Identify Students having number greater than 50
rdd4 = rdd3.filter(lambda x : int(x[1]) >= 50 )
print("Total number of Students Passed : \n {0} \n".format(rdd4.count()))

rdd5 = rdd3.filter(lambda x : int(x[1]) < 50 )
print("Total number of Students Failed : \n {0} \n".format(rdd5.count()))







#print("Total number of Students : \n {0} \n".format(rdd2.count()))

