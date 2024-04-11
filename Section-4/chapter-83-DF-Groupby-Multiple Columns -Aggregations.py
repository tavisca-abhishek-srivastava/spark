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
os.system('cls||system')

# apart from columns in group by all other columns will be part of aggregate function which you want to display
df1 = df.groupBy('gender').sum("marks")
print(df1.show())

# count based upon groupby
df2 = df.groupBy('gender').count()
print(df2.show())
df3 = df.groupBy('course').count()
print(df3.show())

# max for marks based upon groupby
df4 = df.groupBy('gender').max('marks')
print(df4.show())

# min for marks based upon groupby
df5 = df.groupBy('gender').min('marks')
print(df5.show())

# Average for marks based upon GroupBy of 'age'
df6 = df.groupBy('age').avg('marks')
print(df6.show())

# groupby + sorting
df7 = df.groupBy('course','gender').count().sort(df.course) # groupby will return GroupedData object so we need to use count and then we can sort the output based upon course
print(df7.show())

##### to use multiple aggregation as columns in single DataFrame
# df8 = df.groupBy('course','gender').agg(count('*').alias("total_enrollment"),min("marks"),max("marks"), avg("marks")).sort('course')
# print(df8.show())

df8 = df.groupBy("course","gender").agg(count("*").alias("male/female course wise"),min("marks").alias("minimum marks for male/female course wise"),max("marks").alias("maximum marks for male/female course wise"),avg("marks").alias("avg marks for male/female course wise")).sort(df.course)
print(df8.show())

