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
df = ss.read.options( header='True', delemeter=',').csv("./StudentData.csv")
os.system('cls||system')
# used or '|' operator 
df1 = df.filter((df.course == 'DB') | (df.course == 'Cloud'))
print(df1.show())


# using 'isin' operator to check the condition for multiple values
my_courses = ['DB','Cloud','OOP','DSA']
df2 = df.filter((df.course.isin(my_courses)))
print(df2.show())
# using 'startswith' operator to check the condition for  values starting with some string
df3 = df.filter((df.course.startswith('D')))
print("Courses starting with D {0}".format(df3.show()))

# using 'endswith' operator to check the condition for  values ending with some string/char
df4 = df.filter((df.name.endswith('e')))
print("Names ending with e {0}".format(df4.show()))

# using 'contains' operator to check the condition for  values contains string/char
df5 = df.filter((df.name.contains('se')))
print("Names containing 'se' {0}".format(df5.show()))

# using 'like' operator to check the condition for  values contains string/char
df6 = df.filter((df.name.like('%se%')))
print("Names containing 'se'{0}".format(df6.show()))



