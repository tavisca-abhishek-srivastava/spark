from pyspark.sql import SparkSession
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')


ss = SparkSession.builder.appName("First DF App").getOrCreate()
df = ss.read.option("header",True).csv("C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\\Setion-4\\StudentData.csv")
print((df.show()))
