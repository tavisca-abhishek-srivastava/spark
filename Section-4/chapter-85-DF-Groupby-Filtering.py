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
df1 = df.groupBy('gender').count()
print(df1.show())
# filtering 

df2 = df.filter(df.gender == 'Male')
df3 = df2.groupBy("course","gender").agg(count('*').alias("Total Enrollment for Course"))
print("Courses where total number of Entollment is > 70 \n")
df4 = df3.filter(col("Total Enrollment for Course") > 70)
print(df4.show())

