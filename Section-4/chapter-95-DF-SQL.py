from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col 
from pyspark.sql.functions import max , min , sum , avg , mean,count

import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||clear')

ss = SparkSession.builder.appName("Spark SQL").getOrCreate()
ss.sparkContext.setLogLevel('WARN')
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',',inferSchema='True').csv("./StudentData.csv")
os.system('cls||clear')

df.createOrReplaceTempView("Student")

record_df = ss.sql("select * from Student")

print(record_df.show(truncate=False)) 

record_df1 = ss.sql("select course , gender, count(1) as total_students, max(marks), sum(marks)/total_students as Avg  from Student group by course , gender ")

print(record_df1.show(truncate=False)) 