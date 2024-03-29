from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

ss = SparkSession.builder.appName("First DF App").getOrCreate()
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',').csv("C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\\Setion-4\\StudentData.csv")
os.system('cls||system')
# used or '|' operator 
df1 = df.filter((df.course == 'DB') | (df.course == 'Cloud'))
print(df1.show())


# using 'is in' operator to check the condition for multiple values
my_courses = ['DB','Cloud','OOP','DSA']
df2 = df.filter((df.course.isin(my_courses)))
print(df2.show())
# using 'startswith' operator to check the condition for  values starting with some string
df3 = df.filter((df.course.startswith('D')))
print(df3.show())

# using 'endswith' operator to check the condition for  values ending with some string/char
df4 = df.filter((df.name.endswith('e')))
print(df4.show())

# using 'contains' operator to check the condition for  values contains string/char
df5 = df.filter((df.name.contains('se')))
print(df5.show())

# using 'like' operator to check the condition for  values contains string/char
df6 = df.filter((df.name.like('%se%')))
print(df6.show())



