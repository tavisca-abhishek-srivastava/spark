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
rdd2 = rdd1.filter(lambda x: x != header)
os.system('cls||system')
print("Total number of Students : \n {0} \n".format(rdd2.count()))

