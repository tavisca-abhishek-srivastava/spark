from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col 
from pyspark.sql.functions import max , min , sum , avg , mean,count

import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||clear')

ss = SparkSession.builder.appName("First DF App").getOrCreate()
ss.sparkContext.setLogLevel('WARN')
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',',inferSchema='True').csv("./StudentData.csv")
os.system('cls||clear')

# apart from columns in group by all other columns will be part of aggregate function which you want to display
df1 = df.groupBy('course').count()
os.system('cls||clear')
print("number of Student per Course ")
print(df1.show())

#number of male/female per Course
df2 = df.groupBy('course',"gender").count().sort("course")
print("number of male/female per Course ")
print(df2.show())

#
df3 = df.groupBy('course',"age").agg(min("marks").alias("Min-marks"),max("marks").alias("Max-marks"),(avg("marks").alias("Avg-marks") ))
df4 = df3.withColumn("Avg-marks", (col("Avg-marks").cast("Integer")))
print("Min , Max and Avg per Course per age Group ")
print(df4.show())
