from pyspark.sql import SparkSession
import os
import math , random , sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


ss = SparkSession.builder.appName("First DF App").getOrCreate()
ss.sparkContext.setLogLevel('WARN')
#below option is to infer schmea
df = ss.read.options(inferSchema = 'True' , header='True', delemeter=',').csv("./StudentData.csv")
print((df.show()))
print((df.printSchema()))
