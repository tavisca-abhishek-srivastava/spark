from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.types import StructType,StructField , IntegerType,StringType
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
df2 = df1.withColumn("marks",col("marks").cast('Integer'))
df3 = df2.withColumn("average" , ((col("marks")/col("Total_Marks"))*100).cast("Integer"))

df4 = df3.filter((df3.average > 80) & (df3.course == 'OOP') )

print("Students with More than 80% in OOP: \n")
print(df4.show())

print(" \n")

df5 = df3.filter((df3.average > 60) & (df3.course == 'Cloud') )

print("Students with More than 60% in Cloud : \n")
print(df5.show())
