from pyspark.sql import SparkSession
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


ss = SparkSession.builder.appName("First DF App").getOrCreate()
#below option is to infer schmea
df = ss.read.options(inferSchema = 'True' , header='True', delemeter=',').csv("C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\\Setion-4\\StudentData.csv")
print((df.show()))
print((df.printSchema()))
