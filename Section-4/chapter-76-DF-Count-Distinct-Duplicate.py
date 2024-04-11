from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

ss = SparkSession.builder.appName("First DF App").getOrCreate()
ss.sparkContext.setLogLevel('WARN')
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',',inferSchema='True').csv("./StudentData.csv")
os.system('cls||system')
# used or '|' operator 
df1 = df.withColumn("Total_Marks",fn.lit(120))

print(df1.count())
print(" \n")

df2 = df.select("gender","course")
print("distinct {0}".format(df2.distinct().show()))

# 'dropduplicates' is to remove the duplicate value based upon column/s value

df3 = df.dropDuplicates(["gender"])
print(" {0}".format(df3.show()))

# here combination of gender and course will be considered for uniqueness
df4 = df.dropDuplicates(["gender", "course"])
print("dropDuplicates {0}".format(df4.show()))



