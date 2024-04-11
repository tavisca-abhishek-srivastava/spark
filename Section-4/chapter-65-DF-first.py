# from pyspark.sql import SparkSession
# import os
# import sys , math , random
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# os.system('cls||system')


# ss = SparkSession.builder.appName("First DF App").getOrCreate()
# df = ss.read.option("header",True).csv("./StudentData.csv")
# print((df.show()))

from pyspark.sql import SparkSession
import os , sys , math

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

spark_session = SparkSession.builder.appName("First DF App").getOrCreate()
spark_session.sparkContext.setLogLevel('WARN')
df = spark_session.read.option("header",True).csv("./StudentData.csv")
print(df.show())

